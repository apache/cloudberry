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
 * iceberg_meta_engine.h
 *	  The metadata engine interface and its central dispatch.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/meta/iceberg_meta_engine.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ICEBERG_META_ENGINE_H
#define ICEBERG_META_ENGINE_H

/*
 * This is the header an engine implementation includes, and the next one is
 * expected to be C++.  Everything below therefore has to keep C linkage: a C++
 * translation unit that saw these as C++ declarations would emit mangled
 * references and fail to link against the C registry.  The server headers come
 * in through dl_pg_api.h for the same reason.
 */
#include "common/dl_pg_api.h"

#include "common/dl_err.h"
#include "common/dl_kv.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Server, mapping and table options all arrive as plain pairs. */
typedef DlKeyValue MetaKv;

typedef struct MetaCtx {           /* identity + mapping, no credentials in skeleton */
	const char *catalog_name;      /* catalog name within the external catalog */
	const char *namespace_name;    /* PG schema name */
	const char *table_name;
	const MetaKv *catalog_props;   int n_catalog_props;
	const MetaKv *credential_props; int n_credential_props;  /* empty in stub paths */
} MetaCtx;

typedef struct MetaTableDef { const char *schema_json; } MetaTableDef;
typedef struct MetaTable    { char *metadata_location; char *table_uuid; } MetaTable;
/* Opaque in skeleton: */
typedef struct MetaStatistics MetaStatistics;
typedef struct MetaAppendRequest MetaAppendRequest;
typedef struct MetaCommitAppendRequest MetaCommitAppendRequest;
typedef struct MetaUpdateRequest MetaUpdateRequest;
typedef struct MetaCommitUpdateRequest MetaCommitUpdateRequest;
typedef struct MetaStageResult MetaStageResult;
typedef struct MetaCommitResult MetaCommitResult;
typedef struct MetaFilterExpr MetaFilterExpr;
typedef struct MetaFragmentIter MetaFragmentIter;
typedef struct MetaFragmentBatch MetaFragmentBatch;
typedef struct MetaFileGroupIter MetaFileGroupIter;
typedef struct MetaFileGroup MetaFileGroup;
typedef struct MetaFileGroupList MetaFileGroupList;
typedef struct MetaAlterTableRequest MetaAlterTableRequest;
typedef struct MetaTruncateRequest MetaTruncateRequest;

#define DL_META_ENGINE_ABI_VERSION 1

typedef struct IcebergMetaEngine {
	uint32_t abi_version;      /* must equal DL_META_ENGINE_ABI_VERSION */
	uint32_t struct_size;      /* PREFIX-compat: registry only touches fields covered by
								* struct_size; validation bound is the minimal v1 prefix,
								* NOT sizeof(current struct). Tail may only grow. */
	uint64_t capabilities;     /* DL_CAP_* method-family bitmap, see below */
	const char *name;          /* "agent" / "builtin" / "stub" */

	DlErrCode (*load_table)(const MetaCtx *, MetaTable **);
	DlErrCode (*create_table)(const MetaCtx *, const MetaTableDef *, MetaTable **);
	DlErrCode (*drop_table)(const MetaCtx *, bool purge);
	DlErrCode (*table_exists)(const MetaCtx *, bool *);
	DlErrCode (*get_statistics)(const MetaCtx *, int64_t snapshot, MetaStatistics **);

	/* Iceberg single-table OCC: stage (append/update) then commit_*; NOT a cross-table
	 * distributed atomic commit. */
	DlErrCode (*append)(const MetaCtx *, const MetaAppendRequest *, MetaStageResult *);
	DlErrCode (*commit_append)(const MetaCtx *, const MetaCommitAppendRequest *, MetaCommitResult *);
	DlErrCode (*update)(const MetaCtx *, const MetaUpdateRequest *, MetaStageResult *);
	DlErrCode (*commit_update)(const MetaCtx *, const MetaCommitUpdateRequest *, MetaCommitResult *);

	DlErrCode (*get_fragment)(const MetaCtx *, const char *metadata_location,
						  const MetaFilterExpr *, uint32_t batch_hint, MetaFragmentIter **);
	DlErrCode (*plan_file_groups)(const MetaCtx *, const char *plan_options_json, MetaFileGroupIter **);
	DlErrCode (*commit_file_groups)(const MetaCtx *, const MetaFileGroupList *, const char *, MetaCommitResult *);
	DlErrCode (*alter_table)(const MetaCtx *, const MetaAlterTableRequest *, MetaCommitResult *);
	DlErrCode (*truncate_table)(const MetaCtx *, const MetaTruncateRequest *, MetaCommitResult *);

	/* iterator close callbacks are "void cleanup ABI": noexcept, idempotent, never ereport */
	DlErrCode (*fragment_iter_next_batch)(MetaFragmentIter *, MetaFragmentBatch **);
	void      (*fragment_iter_close)(MetaFragmentIter *);
	DlErrCode (*file_group_iter_next)(MetaFileGroupIter *, MetaFileGroup **);
	void      (*file_group_iter_close)(MetaFileGroupIter *);
} IcebergMetaEngine;

#define DL_CAP_TABLE_LIFECYCLE (UINT64CONST(1) << 0) /* load_table, create_table, drop_table, table_exists */
#define DL_CAP_STATISTICS      (UINT64CONST(1) << 1) /* get_statistics */
#define DL_CAP_APPEND          (UINT64CONST(1) << 2) /* append, commit_append */
#define DL_CAP_UPDATE          (UINT64CONST(1) << 3) /* update, commit_update */
#define DL_CAP_GET_FRAGMENT    (UINT64CONST(1) << 4) /* get_fragment, fragment_iter_next_batch, fragment_iter_close */
#define DL_CAP_REWRITE         (UINT64CONST(1) << 5) /* plan_file_groups, commit_file_groups, file_group_iter_next, file_group_iter_close */
#define DL_CAP_ALTER           (UINT64CONST(1) << 6) /* alter_table */
#define DL_CAP_TRUNCATE        (UINT64CONST(1) << 7) /* truncate_table */

extern DlErrCode RegisterMetaEngine(const IcebergMetaEngine *engine);

/*
 * Returns the engine every lake table goes through.  The engine is not
 * selectable: nothing in a table's definition, and no configuration setting,
 * picks between implementations, so a table can never be reinterpreted by a
 * later change.  The vtable indirection remains because the implementation
 * behind it is expected to change -- the Java agent today, an in-process C++
 * one once it exists -- not because a deployment gets to choose.
 */
extern const IcebergMetaEngine *get_meta_engine(void);

extern DlErrCode meta_engine_create_table(const IcebergMetaEngine *, const MetaCtx *,
								  const MetaTableDef *, MetaTable **);
extern DlErrCode meta_engine_drop_table(const IcebergMetaEngine *, const MetaCtx *,
								bool purge);
extern DlErrCode meta_engine_table_exists(const IcebergMetaEngine *, const MetaCtx *, bool *);
extern DlErrCode meta_engine_load_table(const IcebergMetaEngine *, const MetaCtx *, MetaTable **);

/* Remaining method families follow the same central-dispatch pattern in later PRs. */

#ifdef __cplusplus
}
#endif

#endif						/* ICEBERG_META_ENGINE_H */
