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
 * anserfilter.h
 *	  Bloom-filter payload helpers for Anser channels.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/include/anserfilter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ANSERFILTER_H
#define ANSERFILTER_H

#include "postgres.h"

#include "lib/bloomfilter.h"

#define ANSER_BLOOM_PART_MAGIC		0x41424631U /* ABF1 */
#define ANSER_BLOOM_PART_VERSION		1U

/*
 * On-wire framing for a serialized bloom part.  It deliberately does NOT carry
 * the bitset parameters (size / seed / hash count): both the producer and the
 * consumer build the filter with bloom_create from the same plan parameters, so
 * the shape is agreed by construction and never reconstructed from the wire.
 * magic/version guard the framing; part_index/total_parts track the coordinator
 * fold count (surfaced as diagnostics).  Kept as a struct for forward
 * extensibility.
 */
typedef struct AnserBloomPartHeader
{
	uint32		magic;
	uint32		version;
	uint32		part_index;
	uint32		total_parts;
} AnserBloomPartHeader;

extern uint64 AnserBloomSeed(const char *condition_key);
extern bloom_filter *AnserBloomCreate(int64 total_elems,
								  Size max_payload_bytes,
								  uint64 seed);
extern Size AnserBloomSerializedSize(const bloom_filter *filter);
extern bool AnserBloomSerializePart(const bloom_filter *filter,
									uint32 part_index,
									uint32 total_parts,
									void *buffer,
									Size buffer_size,
									Size *payload_len);
extern bloom_filter *AnserBloomDeserializePart(const void *payload,
										  Size payload_len,
										  int64 total_elems,
										  Size max_payload_bytes,
										  uint64 seed,
										  uint32 *part_index,
										  uint32 *total_parts);
extern bool AnserBloomLooksLikePart(const void *payload, Size payload_len);
extern bool AnserBloomFoldPartInPlace(void *acc, Size acc_len,
									  const void *part, Size part_len);

#endif							/* ANSERFILTER_H */
