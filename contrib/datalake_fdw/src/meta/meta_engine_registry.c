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
 * meta_engine_registry.c
 *	  Metadata engine registry, capability checks and dispatch.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/meta/meta_engine_registry.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "meta/iceberg_meta_engine.h"

/* Registered engine pointers are borrowed; each engine must have static lifetime. */
static const IcebergMetaEngine *engines[8];
static int n_engines;

/*
 * v1 minimal prefix = through file_group_iter_close; the registry must never
 * touch fields beyond engine->struct_size. Do NOT compare against
 * sizeof(IcebergMetaEngine) -- that breaks prefix compatibility once the struct
 * grows.
 */
#define DL_META_ENGINE_V1_MIN_SIZE \
	(offsetof(IcebergMetaEngine, file_group_iter_close) + \
	 sizeof(((IcebergMetaEngine *) 0)->file_group_iter_close))

typedef struct DlCapabilityFamily
{
	uint64		bit;
	const char *family;
	size_t		offsets[5];
	int			n;
} DlCapabilityFamily;

static const DlCapabilityFamily capability_families[] =
{
	{DL_CAP_TABLE_LIFECYCLE, "table lifecycle",
		{offsetof(IcebergMetaEngine, load_table),
		 offsetof(IcebergMetaEngine, create_table),
		 offsetof(IcebergMetaEngine, drop_table),
		 offsetof(IcebergMetaEngine, table_exists)}, 4},
	{DL_CAP_STATISTICS, "statistics",
		{offsetof(IcebergMetaEngine, get_statistics)}, 1},
	{DL_CAP_APPEND, "append",
		{offsetof(IcebergMetaEngine, append),
		 offsetof(IcebergMetaEngine, commit_append)}, 2},
	{DL_CAP_UPDATE, "update",
		{offsetof(IcebergMetaEngine, update),
		 offsetof(IcebergMetaEngine, commit_update)}, 2},
	{DL_CAP_GET_FRAGMENT, "get fragment",
		{offsetof(IcebergMetaEngine, get_fragment),
		 offsetof(IcebergMetaEngine, fragment_iter_next_batch),
		 offsetof(IcebergMetaEngine, fragment_iter_close)}, 3},
	{DL_CAP_REWRITE, "rewrite",
		{offsetof(IcebergMetaEngine, plan_file_groups),
		 offsetof(IcebergMetaEngine, commit_file_groups),
		 offsetof(IcebergMetaEngine, file_group_iter_next),
		 offsetof(IcebergMetaEngine, file_group_iter_close)}, 4},
	{DL_CAP_ALTER, "alter",
		{offsetof(IcebergMetaEngine, alter_table)}, 1},
	{DL_CAP_TRUNCATE, "truncate",
		{offsetof(IcebergMetaEngine, truncate_table)}, 1}
};

/*
 * Is the method at this offset present, and set?
 *
 * A method that falls beyond the engine's struct_size is not part of the
 * object at all: reading it would run past what the engine allocated.  Such a
 * method counts as absent, which is exactly what prefix compatibility means --
 * an engine built against an older header stays loadable, and every capability
 * whose family reaches into the missing tail must be left unset.
 */
static bool
meta_engine_method_is_nonnull(const IcebergMetaEngine *engine, size_t offset)
{
	void		(*method)(void);

	if (offset + sizeof(method) > engine->struct_size)
		return false;

	memcpy(&method, (const char *) engine + offset, sizeof(method));
	return method != NULL;
}

DlErrCode
RegisterMetaEngine(const IcebergMetaEngine *engine)
{
	int			i;
	int			j;

	if (engine == NULL)
		return DL_ERR_INVALID_OPTION;
	if (engine->abi_version != DL_META_ENGINE_ABI_VERSION)
		return DL_ERR_INVALID_OPTION;
	if (engine->struct_size < DL_META_ENGINE_V1_MIN_SIZE)
		return DL_ERR_INVALID_OPTION;
	if (engine->name == NULL)
		return DL_ERR_INVALID_OPTION;

	for (i = 0; i < n_engines; i++)
	{
		if (strcmp(engines[i]->name, engine->name) == 0)
			return DL_ERR_ALREADY_EXISTS;
	}
	if (n_engines >= lengthof(engines))
		return DL_ERR_INTERNAL;

	for (i = 0; i < lengthof(capability_families); i++)
	{
		const DlCapabilityFamily *family = &capability_families[i];
		bool				capability_set =
			(engine->capabilities & family->bit) != 0;

		for (j = 0; j < family->n; j++)
		{
			bool		method_is_nonnull =
				meta_engine_method_is_nonnull(engine, family->offsets[j]);

			if (capability_set != method_is_nonnull)
			{
				elog(WARNING,
					 "datalake_fdw: meta engine \"%s\" has an invalid %s capability family",
					 engine->name, family->family);
				return DL_ERR_INVALID_OPTION;
			}
		}
	}

	/*
	 * One engine per build, by design: nothing selects between implementations
	 * at run time, so a second registration would silently decide which one
	 * every table goes through, depending on registration order.
	 */
	if (n_engines > 0)
		return DL_ERR_ALREADY_EXISTS;

	engines[n_engines++] = engine;
	return DL_OK;
}

const IcebergMetaEngine *
get_meta_engine(void)
{
	/*
	 * Exactly one engine is registered, by DatalakeRegisterMetaEngines(); the
	 * array exists so that adding a second implementation later is a matter of
	 * changing what gets registered, not of reworking the call sites.
	 */
	if (n_engines != 1)
		elog(ERROR,
			 "datalake_fdw: expected exactly one metadata engine, found %d",
			 n_engines);

	return engines[0];
}

/*
 * What every dispatch wrapper does before reaching the engine: discard detail
 * recorded by an earlier call, so that a later report can only describe this
 * one, and refuse a method the engine does not advertise.
 *
 * The reset happens before the capability check on purpose.  A refusal produced
 * here has no detail of its own, and leaving an earlier one in place would let
 * it be reported as the cause.
 */
static DlErrCode
meta_engine_enter(const IcebergMetaEngine *engine, uint64 capability)
{
	dl_error_reset();

	if (engine == NULL)
		return DL_ERR_INTERNAL;
	if ((engine->capabilities & capability) == 0)
		return DL_ERR_NOT_SUPPORTED;

	return DL_OK;
}

DlErrCode
meta_engine_create_table(const IcebergMetaEngine *engine, const MetaCtx *ctx,
					 const MetaTableDef *def, MetaTable **out)
{
	DlErrCode	rc = meta_engine_enter(engine, DL_CAP_TABLE_LIFECYCLE);

	if (rc != DL_OK)
		return rc;
	return engine->create_table(ctx, def, out);
}

DlErrCode
meta_engine_drop_table(const IcebergMetaEngine *engine, const MetaCtx *ctx,
				   bool purge)
{
	DlErrCode	rc = meta_engine_enter(engine, DL_CAP_TABLE_LIFECYCLE);

	if (rc != DL_OK)
		return rc;
	return engine->drop_table(ctx, purge);
}

DlErrCode
meta_engine_table_exists(const IcebergMetaEngine *engine, const MetaCtx *ctx,
					 bool *exists)
{
	DlErrCode	rc = meta_engine_enter(engine, DL_CAP_TABLE_LIFECYCLE);

	if (rc != DL_OK)
		return rc;
	return engine->table_exists(ctx, exists);
}

DlErrCode
meta_engine_load_table(const IcebergMetaEngine *engine, const MetaCtx *ctx,
				   MetaTable **out)
{
	DlErrCode	rc = meta_engine_enter(engine, DL_CAP_TABLE_LIFECYCLE);

	if (rc != DL_OK)
		return rc;
	return engine->load_table(ctx, out);
}
