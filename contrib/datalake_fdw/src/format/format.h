/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * format.h
 *	  Reader and writer interfaces for lake table data files.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/format/format.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DL_FORMAT_H
#define DL_FORMAT_H

#include <stdbool.h>
#include <stdint.h>

#include "common/dl_err.h"

/* Arrow C data interface: stable public ABI. */
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

struct ArrowSchema {
	const char *format;
	const char *name;
	const char *metadata;
	int64_t flags;
	int64_t n_children;
	struct ArrowSchema **children;
	struct ArrowSchema *dictionary;
	void (*release)(struct ArrowSchema *);
	void *private_data;
};

struct ArrowArray {
	int64_t length;
	int64_t null_count;
	int64_t offset;
	int64_t n_buffers;
	int64_t n_children;
	const void **buffers;
	struct ArrowArray **children;
	struct ArrowArray *dictionary;
	void (*release)(struct ArrowArray *);
	void *private_data;
};
#endif						/* ARROW_C_DATA_INTERFACE */

/*
 * One unit of read work.  A fragment is a range of row groups rather than a
 * whole file, because that is the granularity a scan can be divided at: several
 * segments can then read one large file at once, which file-at-a-time
 * assignment cannot express.
 */
typedef struct Fragment
{
	const char *path;
	int			first_row_group;	/* 0-based */
	int			n_row_groups;		/* 0 == to the end of the file */
} Fragment;

/*
 * The columns to materialise, as 0-based indexes into the file schema.  A NULL
 * set, or one with no columns, means every column: "read nothing" is not a
 * projection anyone asks for, so it is not worth a second way to say "all".
 */
typedef struct ProjectionSet
{
	const int  *columns;
	int			ncolumns;
} ProjectionSet;

/*
 * A writer holds a whole row group before it can write one, so this is a bound
 * on memory as much as on the file's shape.  It is Parquet's own default
 * maximum, which is what makes it a sane ceiling for any format.
 */
#define DL_MAX_ROW_GROUP_ROWS (1024 * 1024)

typedef struct WriterOptions
{
	const char *compression;	/* format-defined name; NULL for the default */
	int64_t		row_group_size; /* rows per row group; 0 for the default */
} WriterOptions;

typedef struct RowGroupFilterSet RowGroupFilterSet;
typedef struct FileMeta FileMeta;
typedef struct DeleteFileSet DeleteFileSet;

/* Readers/writers are INSTANCES (ops + impl); configuration travels with the instance.
 * No global slots or trampolines, ever. */
typedef struct FormatReader FormatReader;
typedef struct FormatReaderOps {
	/* Each batch yields ArrowArray+ArrowSchema.  A hidden trailing int64 column
	 * carrying the file-row ordinal is what merge-on-read positional deletes
	 * will match against; it arrives with them, so a batch is the projected
	 * columns and nothing else for now. */
	DlErrCode (*next_batch)(FormatReader *, struct ArrowArray *out,
							struct ArrowSchema *schema, bool *eof);
	/* void cleanup ABI: noexcept, never ereport.  Takes the caller's handle so
	 * that it can clear it -- these run on the resource-owner path during
	 * abort, where the same cleanup can be reached twice, and a second call
	 * has to find nothing left rather than a freed reader. */
	void      (*close)(FormatReader **);
} FormatReaderOps;
struct FormatReader { const FormatReaderOps *ops; void *impl; };

typedef struct FormatWriter FormatWriter;
typedef struct FormatWriterOps {
	/*
	 * The batch is consumed whether or not the write succeeds: an
	 * implementation hands it to a library that takes ownership at the call,
	 * and there is no point at which it could hand it back.  The caller is
	 * left with a released ArrowArray either way.
	 */
	DlErrCode (*write_batch)(FormatWriter *, struct ArrowArray *batch);
	/* Rolling support: actual bytes encoded into the sink so far. Valid to query after a
	 * successful write_batch; on failure returns an error code and *out is invalid.
	 * The write.c orchestration layer rolls files (finish -> new open_writer) when this
	 * reaches the soft target.  A format writes in units it cannot split -- a Parquet
	 * row group is one -- and what has not been written is not counted, so the target
	 * is overshot by at most one such unit. */
	DlErrCode (*bytes_written)(FormatWriter *, int64_t *out);
	/* Reportable close-time errors surface ONLY here.  The writer is consumed
	 * and the caller's handle cleared whether or not it succeeds: a file whose
	 * footer could not be written is not one anything can retry against. */
	DlErrCode (*finish)(FormatWriter **, FileMeta **meta);
	/* void cleanup ABI, as for close() above: discards the file being written
	 * and clears the caller's handle. */
	void      (*abort)(FormatWriter **);
} FormatWriterOps;
struct FormatWriter { const FormatWriterOps *ops; void *impl; };

/* Bumped when an existing field changes meaning; appending does not need it. */
#define DL_FORMAT_ABI_VERSION 1

typedef struct FormatRoutine {
	uint32_t abi_version, struct_size;    /* same prefix-compat semantics as meta engine */
	const char *name;                     /* "parquet" */
	DlErrCode (*open_reader)(const Fragment *, const ProjectionSet *,
							 const RowGroupFilterSet *, FormatReader **out);
	DlErrCode (*open_writer)(const char *path, /* TupleDesc */ void *tupdesc,
							 const WriterOptions *, FormatWriter **out);
} FormatRoutine;

extern const FormatRoutine *GetFormatRoutine(const char *format);

/* MoR positional-delete decorator: consumes the inner instance, returns a new instance.
 * close(outer) exactly-once: releases itself then close(inner); idempotent; on open
 * failure the wrapper owns releasing inner. */
extern DlErrCode WrapPositionDeleteFilter(FormatReader *inner, const DeleteFileSet *,
										  FormatReader **out);

#endif						/* DL_FORMAT_H */
