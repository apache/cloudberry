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
 * anserfold_test.c
 *	  What AnserBloomFoldPartInPlace() does to a part header.
 *
 * The coordinator folds every later part of a channel into the first one, and
 * total_parts in that header is how many producers the channel is waiting for:
 * every part carries the same number, and folding must not change it.  It used
 * to be incremented per fold, which left the merged header holding
 * n_producers + (folds) -- 2N-1 for a complete channel, and N only when N is 1.
 * That number is what a consumer reports and what the log prints when a filter
 * goes missing, so it was wrong exactly where someone would be reading it.
 *
 * A regression test rather than a demonstration: the SQL-level fold test could
 * not see this, because it folds one part into one and 2N-1 is N there.  This
 * one folds three, where the two differ.
 *
 * Runs without a cluster -- the fold is a bitwise OR over caller-supplied
 * memory, so the parts here are built by hand rather than by bloom_create().
 * The backend symbols anserfilter.c refers to from its other functions are
 * stubbed below; none of them is on this path, and each aborts if that ever
 * stops being true.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/test/anserfold_test.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdio.h>
#include <stdlib.h>

#include "anserfilter.h"
#include "common/hashfn.h"
#include "lib/bloomfilter.h"
#include "port/pg_bitutils.h"

#define BITSET_BYTES	64
#define PART_BYTES		(sizeof(AnserBloomPartHeader) + BITSET_BYTES)

#define PRODUCERS		3		/* so that N and 2N-1 differ */

static int	failures = 0;

static void
check(bool ok, const char *what)
{
	if (!ok)
	{
		fprintf(stderr, "FAIL: %s\n", what);
		failures++;
	}
}

/*
 * One part as it travels: the header every producer writes, then a bitset with
 * a single byte set, so a union of several is easy to recognise.
 */
static void
make_part(unsigned char *buf, uint32 part_index, uint32 total_parts,
		  int set_byte)
{
	AnserBloomPartHeader *h = (AnserBloomPartHeader *) buf;

	memset(buf, 0, PART_BYTES);
	h->magic = ANSER_BLOOM_PART_MAGIC;
	h->version = ANSER_BLOOM_PART_VERSION;
	h->part_index = part_index;
	h->total_parts = total_parts;
	buf[sizeof(AnserBloomPartHeader) + set_byte] = 0x01;
}

static bool
header_is(const unsigned char *buf, uint32 part_index, uint32 total_parts)
{
	const AnserBloomPartHeader *h = (const AnserBloomPartHeader *) buf;

	return h->magic == ANSER_BLOOM_PART_MAGIC &&
		h->version == ANSER_BLOOM_PART_VERSION &&
		h->part_index == part_index &&
		h->total_parts == total_parts;
}

static bool
bit_set(const unsigned char *buf, int byte)
{
	return buf[sizeof(AnserBloomPartHeader) + byte] == 0x01;
}

int
main(void)
{
	unsigned char acc[PART_BYTES];
	unsigned char second[PART_BYTES];
	unsigned char third[PART_BYTES];
	unsigned char before[PART_BYTES];
	unsigned char bigger[PART_BYTES * 2];
	unsigned char bad[PART_BYTES];

	/*
	 * A three-producer channel: the first part is kept verbatim, the other two
	 * fold into it.  This is the whole point of the file -- with one fold the
	 * old behaviour was indistinguishable from the right one.
	 */
	make_part(acc, 0, PRODUCERS, 0);
	make_part(second, 1, PRODUCERS, 1);
	make_part(third, 2, PRODUCERS, 2);

	check(AnserBloomFoldPartInPlace(acc, PART_BYTES, second, PART_BYTES),
		  "folding a same-sized part is accepted");
	check(header_is(acc, 0, PRODUCERS),
		  "one fold leaves the header alone");

	check(AnserBloomFoldPartInPlace(acc, PART_BYTES, third, PART_BYTES),
		  "folding a third part is accepted");
	check(header_is(acc, 0, PRODUCERS),
		  "two folds leave the header alone (was 2N-1)");

	check(bit_set(acc, 0) && bit_set(acc, 1) && bit_set(acc, 2),
		  "the union carries every part's bits");

	/* The parts folded in are read-only. */
	check(header_is(second, 1, PRODUCERS) && header_is(third, 2, PRODUCERS),
		  "the folded-in parts are untouched");

	/*
	 * A part of another size cannot be OR'd, and the refusal has to leave the
	 * accumulator exactly as it was -- the caller cancels the channel on it.
	 */
	memcpy(before, acc, PART_BYTES);
	make_part(bigger, 0, PRODUCERS, 0);
	check(!AnserBloomFoldPartInPlace(acc, PART_BYTES, bigger, PART_BYTES * 2),
		  "a differently-sized part is refused");
	check(memcmp(acc, before, PART_BYTES) == 0,
		  "a refused size leaves the accumulator byte-identical");

	/* The same for a part whose header does not pass validation. */
	make_part(bad, 0, PRODUCERS, 0);
	((AnserBloomPartHeader *) bad)->magic = 0xDEADBEEFU;
	check(!AnserBloomFoldPartInPlace(acc, PART_BYTES, bad, PART_BYTES),
		  "a part with a bad magic is refused");
	check(memcmp(acc, before, PART_BYTES) == 0,
		  "a refused part leaves the accumulator byte-identical");

	if (failures > 0)
	{
		fprintf(stderr, "%d check(s) failed\n", failures);
		return 1;
	}
	printf("all checks passed\n");
	return 0;
}

/*
 * Stubs for what anserfilter.c's other functions refer to.  The fold path
 * touches none of them; reaching one means this test is no longer testing what
 * it says it is, so they abort rather than return something plausible.
 *
 * Signatures are copied from the declarations the compiler has already seen,
 * so a mismatch is a build error here rather than a surprise at link time.
 */
#define NOT_ON_THIS_PATH(fn) \
	do { \
		fprintf(stderr, "%s called: the fold path is not supposed to\n", fn); \
		abort(); \
	} while (0)

bool		gp_anser_debug = false;

bloom_filter *
bloom_create(int64 total_elems, int bloom_work_mem, uint64 seed)
{
	NOT_ON_THIS_PATH("bloom_create");
}

bloom_filter *
bloom_create_from_bitset(int64 total_elems, int bloom_work_mem, uint64 seed,
						 const unsigned char *bitset, Size bitset_bytes)
{
	NOT_ON_THIS_PATH("bloom_create_from_bitset");
}

void
bloom_free(bloom_filter *filter)
{
	NOT_ON_THIS_PATH("bloom_free");
}

Size
bloom_bitset_bytes(const bloom_filter *filter)
{
	NOT_ON_THIS_PATH("bloom_bitset_bytes");
}

const unsigned char *
bloom_bitset_data(const bloom_filter *filter)
{
	NOT_ON_THIS_PATH("bloom_bitset_data");
}

uint64
bloom_total_bits(bloom_filter *filter)
{
	NOT_ON_THIS_PATH("bloom_total_bits");
}

uint64
hash_bytes_extended(const unsigned char *k, int keylen, uint64 seed)
{
	NOT_ON_THIS_PATH("hash_bytes_extended");
}

uint64
pg_popcount(const char *buf, int bytes)
{
	NOT_ON_THIS_PATH("pg_popcount");
}

/* ANSER_DEBUG expands to elog(); gp_anser_debug is off, so it never fires. */
bool
errstart(int elevel, const char *domain)
{
	return false;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return false;
}

void
errfinish(const char *filename, int lineno, const char *funcname)
{
}

void
errmsg_internal(const char *fmt,...)
{
}
