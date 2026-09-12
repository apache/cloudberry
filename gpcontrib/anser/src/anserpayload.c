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
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * anserpayload.c
 *	  The registry of payload types.
 *
 * One row per kind of runtime information a channel can carry.  See
 * anserpayload.h for what a row means and how to add one.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/src/anserpayload.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "anserfilter.h"
#include "anserpayload.h"

/*
 * Indexed by the wire byte itself, so a lookup is one load and registering a
 * type is one line (the pattern syscache.c:154 uses for its cache
 * descriptors).  Unregistered slots are all-zero, which is the answer we want
 * for every question asked of them: no fold, no checksum, no name.
 *
 * Only two of the 256 slots are used, so this costs 8 KB of read-only data to
 * hold mostly zeroes.  That is the whole point: the alternative ways to reach a
 * descriptor in constant time -- a second array mapping code to row, or a
 * switch -- are extra code that has to be kept in step with the table, and the
 * 8 KB buys them all away.  A linear search over a two-row table would be just
 * as fast in practice; what it would not be is impossible to get wrong.
 */
static const AnserPayloadOps AnserPayloadTable[PG_UINT8_MAX + 1] =
{
	/*
	 * A subscription: header and key only.  It has no body to fold or to
	 * checksum, but its header still is checksummed -- that is what protects
	 * the condition key it is asking to be served on.
	 */
	[(unsigned char) ANSER_PAYLOAD_NONE] = {
		ANSER_PAYLOAD_NONE, "none", false, NULL, NULL
	},

	/*
	 * A bloom filter part.  AnserBloomFoldPartInPlace already has exactly the
	 * fold contract -- it validates both sides before touching the accumulator
	 * and ORs the bitsets -- so it is used directly rather than wrapped, as is
	 * the fill check for worth_delivering.
	 */
	[(unsigned char) ANSER_PAYLOAD_BLOOM] = {
		ANSER_PAYLOAD_BLOOM, "bloom", true, AnserBloomFoldPartInPlace,
		AnserBloomPartWorthSending
	}
};

/*
 * Resolve a code, or NULL if nothing has registered it.
 *
 * The NULL matters: 'code' reaches us straight off the wire, so "no such type"
 * is a case that has to be reportable rather than silently defaulted.  It is
 * also the diagnostic that catches a payload type given a code in the header
 * but no row in the table.
 */
const AnserPayloadOps *
AnserPayloadLookup(char code)
{
	/*
	 * Cast before indexing.  'char' is signed on x86, and the code is an
	 * untrusted byte: without this, a 0x80-0xff code from a corrupted or
	 * hostile message would index the table at a negative offset.
	 */
	const AnserPayloadOps *ops = &AnserPayloadTable[(unsigned char) code];

	if (ops->code == '\0')
		return NULL;

	/* The index is authoritative; a row filed under the wrong code is a typo. */
	Assert(ops->code == code);

	return ops;
}

bool
AnserPayloadChecksumsBody(char code)
{
	/*
	 * No presence check: an unregistered code lands on a zeroed slot, and
	 * "false" is exactly right for it -- there is nothing to checksum that we
	 * would know how to use anyway.
	 */
	return AnserPayloadTable[(unsigned char) code].checksum_body;
}
