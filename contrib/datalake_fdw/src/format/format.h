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

typedef struct Fragment Fragment;                 /* opaque in skeleton */
typedef struct ProjectionSet ProjectionSet;
typedef struct RowGroupFilterSet RowGroupFilterSet;
typedef struct WriterOptions WriterOptions;
typedef struct FileMeta FileMeta;
typedef struct DeleteFileSet DeleteFileSet;

/* Readers/writers are INSTANCES (ops + impl); configuration travels with the instance.
 * No global slots or trampolines, ever. */
typedef struct FormatReader FormatReader;
typedef struct FormatReaderOps {
	/* Each batch yields ArrowArray+ArrowSchema; last column is a hidden int64 file-row
	 * ordinal (for MoR positional deletes). */
	DlErrCode (*next_batch)(FormatReader *, struct ArrowArray *out,
							struct ArrowSchema *schema, bool *eof);
	void      (*close)(FormatReader *);   /* void cleanup ABI: noexcept, idempotent, never ereport */
} FormatReaderOps;
struct FormatReader { const FormatReaderOps *ops; void *impl; };

typedef struct FormatWriter FormatWriter;
typedef struct FormatWriterOps {
	DlErrCode (*write_batch)(FormatWriter *, struct ArrowArray *batch); /* success == consumed */
	/* Rolling support: actual bytes encoded into the sink so far. Valid to query after a
	 * successful write_batch; on failure returns an error code and *out is invalid.
	 * The write.c orchestration layer rolls files (finish -> new open_writer) when this
	 * reaches the soft target; overshoot of at most one batch is allowed. */
	DlErrCode (*bytes_written)(FormatWriter *, int64_t *out);
	DlErrCode (*finish)(FormatWriter *, FileMeta **meta);  /* reportable close-time errors
																		* surface ONLY here */
	void      (*abort)(FormatWriter *);   /* void cleanup ABI: noexcept, idempotent, never ereport */
} FormatWriterOps;
struct FormatWriter { const FormatWriterOps *ops; void *impl; };

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
