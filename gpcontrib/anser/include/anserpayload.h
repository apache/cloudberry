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
 * anserpayload.h
 *	  What a channel carries, and who knows what to do with it.
 *
 * The transport (ansersideband.c on segments, anserdispatch.c on the
 * coordinator) moves opaque bytes: it cannot tell a bloom filter from a row
 * count, and should not have to.  Everything that depends on what the bytes
 * mean lives in one descriptor per payload type, so a new kind of runtime
 * information needs no change to the framing, the checksumming or the channel
 * bookkeeping.
 *
 * Exactly two things genuinely differ per type.
 *
 * How parts merge.  The coordinator receives one part per producer and must
 * reduce them to a single payload.  For a bloom filter that is a bitwise OR of
 * equal-sized bitsets; for a row count it would be a sum; for a min/max
 * summary it would be two comparisons.  There is no generic answer, so each
 * type supplies fold().
 *
 * Whether corruption can give a wrong answer.  This is what 'checksum_body'
 * decides, and it is about consequence, not size.  A bloom filter fails
 * asymmetrically: a bit flipped 0 -> 1 merely costs selectivity, but a bit
 * flipped 1 -> 0 removes a key from the filter, so a joinable row is rejected
 * and the query returns fewer rows than it should -- silently.  A row count, by
 * contrast, only feeds a planning decision: a corrupted one costs a worse plan,
 * never an incorrect result.  It does not need to be checksummed, and paying
 * 0.05 ms/MB to checksum it anyway would be waste.
 *
 * Note what 'checksum_body' does *not* cover: the header and the condition key
 * are checksummed for every message regardless.  That is ~100 ns for a 95-byte
 * header and it is what validates the routing fields, so there is nothing to
 * gain by skipping it; the flag controls only the payload, which is where the
 * cost lives.
 *
 * To add a payload type:
 *
 *   1. #define a code below.  It travels as one byte and appears in the
 *      anser.debug trace, so keep it printable -- and it may not be '\0',
 *      which AnserPayloadTable uses to recognize a slot nobody registered.
 *   2. Write a fold() and add a row to AnserPayloadTable in anserpayload.c,
 *      keyed by the code: the table is indexed by the wire byte, so the
 *      designator and the row's own 'code' field must agree (an assertion in
 *      AnserPayloadLookup catches it if they do not).
 *   3. Pass the code to AnserSidebandPublish/AnserDispatchLocalPublish in the
 *      producer node, and to AnserSidebandConsumeWait/AnserDispatchLocalConsume
 *      in the consumer node.
 *
 * That is the whole extension point.  In particular, do not add a branch on the
 * payload type to the transport: if something there needs to know the type,
 * that knowledge belongs in this descriptor instead.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/include/anserpayload.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ANSERPAYLOAD_H
#define ANSERPAYLOAD_H

#include "anser.h"

/*
 * Payload type codes.
 *
 * Distinct from the message kind (P/S), which says what a message *does*
 * rather than what it *carries*: a subscription carries nothing, and a
 * cancelled part still declares the type it would have carried.
 */
#define ANSER_PAYLOAD_NONE		'-'		/* no payload (a subscription) */
#define ANSER_PAYLOAD_BLOOM		'B'		/* serialized bloom filter part */

/* '\0' is reserved: it is how an unregistered code is recognized. */

typedef struct AnserPayloadOps
{
	char		code;			/* the wire byte */
	const char *name;			/* for logs; no other use */
	bool		checksum_body;	/* see the file header */

	/*
	 * Merge 'part' into the accumulator in place.
	 *
	 * The accumulator is the first part the channel received, kept verbatim, so
	 * the two always have the same type -- but not necessarily the same length
	 * or internal parameters, which is what this has to check.  Return false if
	 * they cannot be merged; the caller then cancels the channel, which costs
	 * the consumers their filter but is never silently wrong.
	 *
	 * Must not modify 'accum' unless it returns true: the caller reports a
	 * false return and carries on, so a half-folded accumulator would be
	 * indistinguishable from a good one.
	 *
	 * NULL for a type that is never folded (one that has no payload).
	 */
	bool	  (*fold) (void *accum, Size accum_len,
					   const void *part, Size part_len);

	/*
	 * Is the finished accumulator worth delivering?  Asked once, after the last
	 * part has been folded and before anything is sent to a consumer; a false
	 * return cancels the channel, so consumers run unfiltered.
	 *
	 * This is the only place the question can be answered honestly, because
	 * folding changes the answer: OR-ing bloom parts together makes the result
	 * denser than any part, so three parts that each looked worth sending can
	 * merge into one that is not.  Producers cannot see that, and consumers
	 * should not be made to pay for it.
	 *
	 * NULL for a type that is always worth delivering -- a row count does not
	 * become less useful for having been summed.
	 */
	bool	  (*worth_delivering) (const void *accum, Size accum_len);
} AnserPayloadOps;

/*
 * Resolve a code to its descriptor, or NULL if nothing has registered it.
 *
 * Callers must handle the NULL: the code arrives from the wire, so an
 * unregistered one is a real case, and defaulting it silently would turn a
 * forgotten table row into a mystery rather than a log line.
 */
extern const AnserPayloadOps *AnserPayloadLookup(char code);

/*
 * Whether a type's body is covered by the message checksum.  Takes a code
 * rather than a descriptor because the transport reaches this while parsing,
 * before it has resolved anything; an unregistered code answers false, which
 * is why this one needs no error handling.
 */
extern bool AnserPayloadChecksumsBody(char code);

#endif							/* ANSERPAYLOAD_H */
