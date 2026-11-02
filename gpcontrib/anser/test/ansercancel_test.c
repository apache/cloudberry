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
 * ansercancel_test.c
 *    Immediate payload release on every channel cancellation path.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/test/ansercancel_test.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <stdlib.h>
/* Do not link backend logging, but preserve ERROR's non-returning behavior.
 * Silencing ERROR too would hide invalid paths and confuse flow analysis in
 * backend inline functions included by the transport source. */
#undef elog
#define elog(level, ...) do { if ((level) >= ERROR) abort(); } while (0)
#include "../src/anserdispatch.c"

#define CHECK(c) do { if (!(c)) abort(); } while (0)
#define PAYLOAD_SIZE (1024 * 1024)

static void *owned;
static Size live_bytes;
static int releases;
static bool accept_fold;
static bool accept_delivery;

void *palloc(Size bytes)
{
	CHECK(owned == NULL && live_bytes == 0);
	owned = malloc(bytes);
	CHECK(owned != NULL);
	live_bytes = bytes;
	return owned;
}
void pfree(void *ptr)
{
	CHECK(ptr != NULL && ptr == owned);
	free(ptr);
	owned = NULL;
	live_bytes = 0;
	++releases;
}
void ExceptionalCondition(const char *a, const char *b, int c) { abort(); }

static bool
fold(void *accum, Size accum_len, const void *part, Size part_len)
{
	CHECK(accum == owned && accum_len == PAYLOAD_SIZE);
	return accept_fold;
}
static bool
worth_delivering(const void *accum, Size accum_len)
{
	CHECK(accum == owned && accum_len == PAYLOAD_SIZE);
	return accept_delivery;
}

int main(void)
{
	AnserPayloadOps ops = {0};
	AnserPayloadOps different;
	char *part = malloc(PAYLOAD_SIZE);
	int iteration;
	int reason;

	CHECK(part != NULL);
	memset(part, 1, PAYLOAD_SIZE);
	ops.name = "fixture";
	ops.fold = fold;
	ops.worth_delivering = worth_delivering;
	different = ops;
	different.name = "different";
	for (iteration = 0; iteration < 5; ++iteration)
		for (reason = 0; reason < 5; ++reason)
		{
			AnserDispChannel channel = {0};
			int before = releases;

			accept_fold = accept_delivery = true;
			ops.fold = fold;
			anser_disp_apply_part(&channel, &ops, part, PAYLOAD_SIZE, 2, false);
			CHECK(channel.payload == owned && live_bytes == PAYLOAD_SIZE);
			CHECK(!channel.complete && !channel.cancelled);
			CHECK(memcmp(channel.payload, part, PAYLOAD_SIZE) == 0);
			switch (reason)
			{
				case 0: /* explicit cancellation after an accumulated part */
					anser_disp_apply_part(&channel, &ops, NULL, 0, 2, true);
					break;
				case 1: /* producer payload types disagree */
					anser_disp_apply_part(&channel, &different, part, PAYLOAD_SIZE, 2, false);
					break;
				case 2: /* incompatible part */
					accept_fold = false;
					anser_disp_apply_part(&channel, &ops, part, PAYLOAD_SIZE, 2, false);
					break;
				case 3: /* merged payload is not worth delivering */
					accept_delivery = false;
					anser_disp_apply_part(&channel, &ops, part, PAYLOAD_SIZE, 2, false);
					break;
				case 4: /* no fold function */
					ops.fold = NULL;
					anser_disp_apply_part(&channel, &ops, part, PAYLOAD_SIZE, 2, false);
					break;
			}
			CHECK(channel.cancelled && channel.complete);
			CHECK(channel.payload == NULL && channel.payload_len == 0);
			CHECK(live_bytes == 0 && releases == before + 1);
			/* Late parts and repeated cancellation must not double-free. */
			anser_disp_apply_part(&channel, &ops, part, PAYLOAD_SIZE, 2, false);
			anser_disp_apply_part(&channel, &ops, NULL, 0, 2, true);
			CHECK(live_bytes == 0 && releases == before + 1);
		}
	/* Cancellation before the first payload must not call pfree(NULL). */
	{
		AnserDispChannel empty = {0};
		int before = releases;

		anser_disp_apply_part(&empty, &ops, NULL, 0, 2, true);
		CHECK(empty.cancelled && empty.complete && releases == before);
	}
	/* A useful completed payload stays owned by the channel until reset. */
	{
		AnserDispChannel control = {0};
		int before = releases;

		ops.fold = fold;
		accept_fold = accept_delivery = true;
		anser_disp_apply_part(&control, &ops, part, PAYLOAD_SIZE, 2, false);
		anser_disp_apply_part(&control, &ops, part, PAYLOAD_SIZE, 2, false);
		CHECK(control.complete && !control.cancelled);
		CHECK(control.payload == owned && live_bytes == PAYLOAD_SIZE);
		CHECK(releases == before);
		pfree(control.payload); /* command-reset ownership */
		CHECK(live_bytes == 0 && releases == before + 1);
	}
	free(part);
	return 0;
}
