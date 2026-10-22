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
 * stub_engine.c
 *	  A metadata engine that reports what it would have done.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/meta/engine_stub/stub_engine.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "meta/engine_stub/stub_engine.h"
#include "meta/iceberg_meta_engine.h"

static DlErrCode
stub_create_table(const MetaCtx *ctx, const MetaTableDef *def, MetaTable **out)
{

	ereport(NOTICE,
			(errmsg("stub engine: created iceberg table \"%s.%s\" in catalog \"%s\"",
					ctx->namespace_name, ctx->table_name, ctx->catalog_name)));
	*out = NULL;
	return DL_OK;
}

static DlErrCode
stub_drop_table(const MetaCtx *ctx, bool purge)
{
	/*
	 * Report whether the data would have gone with it, so that a regression can
	 * see which of the two things a DROP asked for.
	 */
	ereport(NOTICE,
			(errmsg("stub engine: dropped iceberg table \"%s.%s\" from catalog \"%s\"%s",
					ctx->namespace_name, ctx->table_name, ctx->catalog_name,
					purge ? ", purging data" : ", keeping data")));
	return DL_OK;
}

static DlErrCode
stub_table_exists(const MetaCtx *ctx, bool *exists)
{

	/* The stub has no remote catalog. */
	*exists = false;
	return DL_OK;
}

/*
 * Nothing to load in the skeleton; the method remains a member of the lifecycle
 * family, so it exists and refuses.
 *
 * It refuses the way a real engine has to: the code says what kind of failure it
 * is, and everything specific to this failure -- which table, which operation,
 * what the implementation calls it -- is recorded for the reporting layer.  A
 * remote engine records the message and stack it received here instead.
 */
static DlErrCode
stub_load_table(const MetaCtx *ctx, MetaTable **out)
{
	dl_error_set(DL_ERR_NOT_SUPPORTED, "load_table", "StubEngine",
				 psprintf("the stub engine holds no metadata for \"%s.%s\"",
						  ctx->namespace_name, ctx->table_name));
	return DL_ERR_NOT_SUPPORTED;
}

static const IcebergMetaEngine stub_engine =
{
	.abi_version = DL_META_ENGINE_ABI_VERSION,
	.struct_size = sizeof(IcebergMetaEngine),
	.capabilities = DL_CAP_TABLE_LIFECYCLE,
	.name = "stub",
	.load_table = stub_load_table,
	.create_table = stub_create_table,
	.drop_table = stub_drop_table,
	.table_exists = stub_table_exists
};

DlErrCode
RegisterStubMetaEngine(void)
{
	return RegisterMetaEngine(&stub_engine);
}
