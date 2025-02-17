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
 * sm3.h
 *
 * IDENTIFICATION
 *          contrib/pgcrypto/sm3.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _SM3_H_
#define _SM3_H_

#include "postgres.h"

#include <time.h>

#include "common/cryptohash.h"
#include "common/sha2.h"
#include "px.h"

void		init_sm3(PX_MD *h);

/* SM3 */
static unsigned
int_sm3_len(PX_MD *h)
{
	return PG_SM3_DIGEST_LENGTH;
}

static unsigned
int_sm3_block_len(PX_MD *h)
{
	return PG_SM3_BLOCK_LENGTH;
}

static void
int_sm3_update(PX_MD *h, const uint8 *data, unsigned dlen)
{
	pg_cryptohash_ctx *ctx = (pg_cryptohash_ctx *) h->p.ptr;

	if (pg_cryptohash_update(ctx, data, dlen) < 0)
		elog(ERROR, "could not update %s context", "SM3");
}

static void
int_sm3_reset(PX_MD *h)
{
	pg_cryptohash_ctx *ctx = (pg_cryptohash_ctx *) h->p.ptr;

	if (pg_cryptohash_init(ctx) < 0)
		elog(ERROR, "could not initialize %s context", "SM3");
}

static void
int_sm3_finish(PX_MD *h, uint8 *dst)
{
	pg_cryptohash_ctx *ctx = (pg_cryptohash_ctx *) h->p.ptr;

	if (pg_cryptohash_final(ctx, dst, h->result_size(h)) < 0)
		elog(ERROR, "could not finalize %s context", "SM3");
}

static void
int_sm3_free(PX_MD *h)
{
	pg_cryptohash_ctx *ctx = (pg_cryptohash_ctx *) h->p.ptr;

	pg_cryptohash_free(ctx);
	pfree(h);
}

void
init_sm3(PX_MD *md)
{
	pg_cryptohash_ctx *ctx;

	ctx = pg_cryptohash_create(PG_SM3);
	md->p.ptr = ctx;

	md->result_size = int_sm3_len;
	md->block_size = int_sm3_block_len;
	md->reset = int_sm3_reset;
	md->update = int_sm3_update;
	md->finish = int_sm3_finish;
	md->free = int_sm3_free;

	md->reset(md);
}

#endif