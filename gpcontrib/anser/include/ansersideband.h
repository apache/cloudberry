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
 * ansersideband.h
 *	  Anser transport over the existing QD <-> QE dispatch connection.
 *
 * Instead of opening a second libpq connection back to the coordinator, a
 * segment executor reuses the connection the dispatcher already holds open:
 * QE -> QD travels as a NOTIFY (the model nextval() uses, see
 * cdb_sequence_nextval_qe in commands/sequence.c), and QD -> QE as a
 * GP_SIDEBAND_MESSAGE the waiting QE reads off its own socket.
 *
 * The two directions are not symmetric, and the wire formats differ for a
 * reason.  A NOTIFY payload is delivered by pq_sendstring(), so it must be a
 * NUL-free C string -- hence the text header and base64 body.  The QD -> QE
 * push is written with pqPutnchar(), which performs no conversion, so the
 * merged filter travels as raw binary.  That matters: the merged payload is
 * sent once per consumer, while each part is sent once.
 *
 * Both directions carry a CRC32C.  Not because the transport is lossy -- it is
 * TCP, or a Unix socket; the UDP interconnect is a different channel entirely
 * -- but because some payloads fail asymmetrically and TCP's 16-bit checksum is
 * thin cover for a megabyte.  The header and condition key are always covered;
 * whether the body is covered too is the payload type's decision
 * (AnserPayloadOps.checksum_body, see anserpayload.h), because that is the part
 * that costs 0.05 ms/MB and only some payloads can turn corruption into a wrong
 * answer.  The QE -> QD checksum covers the raw pre-base64 bytes, so where it
 * applies it validates the decode as well.
 *
 * Neither direction branches on the payload type beyond consulting that one
 * flag: everything else about what the bytes mean belongs in the registry.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/include/ansersideband.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ANSERSIDEBAND_H
#define ANSERSIDEBAND_H

#include "anser.h"
#include "anserpayload.h"
#include "port/pg_crc32c.h"

struct CdbDispatchResult;		/* #include "cdb/cdbdispatchresult.h" */
struct pgNotify;				/* #include "libpq-fe.h" */

/*
 * NOTIFY channel for QE -> QD Anser traffic.  The dispatcher hands notifies it
 * does not recognize to cdbdisp_notify_hook, where we match on this name.
 */
#define ANSER_NOTIFY_CHANNEL	"anser_rf"

/* First token of every QE -> QD payload; bump when the format changes. */
#define ANSER_WIRE_TAG			"anser3"

/*
 * Message kinds (QE -> QD): what the message does.  What it carries is a
 * separate field, the payload type -- see anserpayload.h.
 */
#define ANSER_WIRE_KIND_PART		'P'		/* a producer's serialized part */
#define ANSER_WIRE_KIND_SUBSCRIBE	'S'		/* a consumer registering interest */

/* Flag bits, shared by both directions. */
#define ANSER_WIRE_F_CANCELLED	0x0001

/*
 * QE -> QD header: fixed width, so the key and body start at known offsets and
 * nothing about their contents can affect parsing.
 *
 *   anser3 K T SSSSSSSSSS CCCCCCCCCC DDDDDDDDDD PPPPPPPPPP TTTTTTTTTT FFFF KKKK BBBBBBBBBB XXXXXXXX
 *   ^tag   ^ ^  session    command    condition  part       total      ^    ^    bodylen    crc
 *          |  \ payload type                                       flags    keylen
 *           \ kind
 *
 * followed immediately by <keylen> key bytes and <bodylen> body bytes.  Every
 * field is zero-padded (flags and CRC are hex, the rest decimal), so the header
 * is pure ASCII of a constant length and holds no delimiter that payload bytes
 * could imitate.  The two lengths are then a cross-check: the total payload
 * length must be exactly the header plus both, or the message is rejected.
 *
 * The signed session and command ids travel as uint32 and are cast back on
 * receipt, which round-trips them exactly while keeping the width fixed --
 * "%010d" of a negative value is eleven characters, which would have made the
 * header variable-width again.
 *
 * The previous format ended its header with a newline and found it with
 * strchr().  That was correct only because the header could not itself contain
 * a newline -- an invariant nothing enforced, and one that a key derived from
 * relation names (quoted identifiers may contain anything) would have broken
 * silently.
 */
#define ANSER_WIRE_HDR_FORMAT \
	ANSER_WIRE_TAG " %c %c %010u %010u %010u %010u %010u %04x %04u %010u %08x"

/*
 * Read format for the same header.  The widths are what make the parse safe:
 * every conversion stops after exactly its field, so no value can consume the
 * separator or run into the key bytes that follow the header.
 */
#define ANSER_WIRE_HDR_SCANF \
	ANSER_WIRE_TAG " %c %c %10u %10u %10u %10u %10u %4x %4u %10u %8x"

/* 6 + the 11 fields above, each preceded by its separating space. */
#define ANSER_WIRE_HDR_LEN		(6 + (1 + 1) + (1 + 1) + (1 + 10) + (1 + 10) \
								 + (1 + 10) + (1 + 10) + (1 + 10) + (1 + 4) \
								 + (1 + 4) + (1 + 10) + (1 + 8))

/* Offset of the 8-hex-digit CRC; the checksum covers everything before it. */
#define ANSER_WIRE_CRC_OFFSET	(ANSER_WIRE_HDR_LEN - 8)

/*
 * Largest key and body a header can describe.  Checked before formatting so a
 * field can never overflow its width and shift every field after it.
 */
#define ANSER_WIRE_MAX_KEYLEN	9999
#define ANSER_WIRE_MAX_BODYLEN	999999999

/*
 * The codec.
 *
 * Four functions, exported rather than static because together they *are* the
 * protocol: the two directions have to agree byte for byte about offsets and
 * about what the checksum covers, and that agreement is only auditable -- and
 * only testable -- if both halves are reachable by name.  sql/anser_test.sql
 * drives them directly.
 *
 * They are implemented on the side that writes: AnserWireFormat in
 * ansersideband.c (only a QE formats), AnserWireParse and AnserWireCheckCrc in
 * anserdispatch.c (only the QD parses).
 *
 * AnserWireFormat returns a palloc'd NUL-terminated string.  AnserWireParse
 * fills 'out' and returns false for anything it will not vouch for; its
 * contract and the checks it makes are documented at the definition.
 * AnserWireCheckCrc takes the *decoded* body, or NULL/0 when there is none.
 */
typedef struct AnserWireMsg
{
	const char *wire;			/* the message itself; the CRC covers its head */
	char		kind;
	char		payload_type;	/* resolved against the registry by the caller */
	AnserChannelKey key;
	int			key_len;
	int			part_index;
	int			total_parts;
	int			flags;
	const char *body;			/* base64, not NUL-terminated */
	int			body_len;
	uint32		crc;			/* of the header, key and decoded body */
} AnserWireMsg;

extern char *AnserWireFormat(const AnserChannelKey *channel_key, char kind,
							 char payload_type, uint32 part_index,
							 uint32 total_parts, int flags,
							 const void *payload, Size payload_len);
extern bool AnserWireParse(const char *msg, AnserWireMsg *out);
extern bool AnserWireCheckCrc(const AnserWireMsg *msg, const void *body,
							  Size body_len);

/*
 * QD -> QE push: raw binary, written with pqPutInt/pqPutnchar.
 *
 *   payload_type | crc32c | condition_id | flags | keylen | key | bodylen | body
 *
 * with each field a 4-byte integer except the key and body.  The CRC covers all
 * of the other fields in that order, the integers in network byte order, so it
 * validates the routing information as well as the payload; the body is
 * included only when the payload type says so.  Both ends compute it through
 * this one function so the two cannot drift apart.  It is implemented in
 * ansersideband.c, beside the reader that verifies it.
 */
extern pg_crc32c AnserWirePushCrc(char payload_type, uint32 condition_id,
								  uint32 flags, const char *key, int keylen,
								  const void *body, int bodylen);

/*
 * QE side (ansersideband.c).
 *
 * AnserSidebandPublish is fire-and-forget: unlike the libpq transport it does
 * not wait for the coordinator to acknowledge the part.  'payload_type' is
 * stamped on the wire even when cancelled, since it is what tells the
 * coordinator how to fold the channel's parts.
 *
 * AnserSidebandConsumeWait blocks on this backend's own dispatch socket until
 * the merged payload arrives, the channel is cancelled, or timeout_ms elapses.
 * On success *payload is palloc'd in the caller's context.  A false return
 * always means "run unfiltered", never an error -- including when what arrived
 * is not of 'payload_type', which would mean two different kinds of information
 * had collided on one channel.
 */
extern bool AnserSidebandPublish(const AnserChannelKey *channel_key,
								 char payload_type,
								 uint32 part_index, uint32 total_parts,
								 const void *payload, Size payload_len,
								 bool cancelled);
extern bool AnserSidebandConsumeWait(const AnserChannelKey *channel_key,
									 char payload_type,
									 void **payload, Size *payload_len,
									 bool *cancelled, long timeout_ms);

/*
 * QD side (anserdispatch.c).
 *
 * AnserDispatchNotifyHandler is installed as cdbdisp_notify_hook; it folds
 * arriving parts and pushes the merged payload to subscribers.  The Local
 * variants serve producers and consumers running on the coordinator itself,
 * which have no dispatch connection to themselves and so operate on the same
 * per-query channel table directly.  They take the same payload type for the
 * same reasons, so that a coordinator-local producer and a segment producer are
 * interchangeable on one channel.
 */
extern bool AnserDispatchNotifyHandler(struct CdbDispatchResult *dispatchResult,
									   struct pgNotify *notify);
extern bool AnserDispatchLocalPublish(const AnserChannelKey *channel_key,
									  char payload_type,
									  uint32 part_index, uint32 total_parts,
									  const void *payload, Size payload_len,
									  bool cancelled);
extern bool AnserDispatchLocalConsume(const AnserChannelKey *channel_key,
									  char payload_type,
									  void **payload, Size *payload_len,
									  bool *cancelled);

/*
 * Drop per-query state.  AnserSidebandResetAll does both sides and is what
 * the executor/transaction-end callbacks call; the halves are exposed because
 * each lives with the state it owns.
 */
extern void AnserDispatchReset(void);
extern void AnserSidebandResetInbox(void);
extern void AnserSidebandResetAll(void);

#endif							/* ANSERSIDEBAND_H */
