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
 * anserdispatch.c
 *	  Coordinator side of the dispatch-connection transport.
 *
 * Everything here runs in the QD backend, reached from cdbdisp_notify_hook
 * while the dispatcher drains QE messages -- which happens from inside the
 * interconnect wait loop, so this code is on the query's critical path.  It
 * must stay cheap and must not throw for anything recoverable: an error here
 * lands in a running query, whereas losing a filter only costs us an
 * unfiltered scan.
 *
 * Because producer merge and consumer delivery both happen in this one
 * process, the accumulator is an ordinary palloc'd buffer.  There is no shared
 * memory, no DSM segment to attach, and no separate worker to hand data to;
 * parts are folded in place as they arrive, so only the final fold is on the
 * critical path.
 *
 * Nothing here knows what a part contains.  Merging is delegated to the payload
 * type's fold() (anserpayload.h), and the only other thing this file asks about
 * a type is whether its body is covered by the message checksum -- which is why
 * it does not include anserfilter.h at all.
 *
 * A note on libpq linkage.  The connections we write to were created by the
 * copy of libpq that is statically linked into the postgres binary, and every
 * symbol listed in libpq's exports.txt is deliberately made *local* in that
 * binary (see the version-script hack in src/backend/Makefile) precisely so a
 * module cannot end up driving one connection through two copies of libpq.
 * So this file must not call the public PQ* API: linking libpq.so to obtain it
 * would create exactly the mixture that hack exists to prevent.  The internal
 * pq* writers are not in exports.txt and so remain global in the backend,
 * which is how pqPutMsgStart() and friends resolve here; the two accessors we
 * would otherwise want are inlined below, straight off the struct.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/src/anserdispatch.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq-fe.h"
#include "libpq-int.h"

#include "anser.h"
#include "anserpayload.h"
#include "ansersideband.h"
#include "cdb/cdbconn.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "common/base64.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

/*
 * One channel's merge state, living for the duration of the query that created
 * it.  Keyed by AnserChannelKey, which must be the first field.
 */
typedef struct AnserDispChannel
{
	AnserChannelKey key;
	const AnserPayloadOps *ops;	/* what this carries; NULL until a part says */
	char	   *payload;		/* merged part, or NULL before the first one */
	Size		payload_len;
	int			parts_received;
	int			expected_parts;	/* 0 until a part tells us; from total_parts */
	bool		cancelled;
	bool		complete;		/* every expected part folded, or cancelled */
	List	   *subscribers;	/* PGconn * of QEs awaiting delivery */
} AnserDispChannel;

static HTAB *AnserDispChannels = NULL;
static MemoryContext AnserDispContext = NULL;

static AnserDispChannel *anser_disp_lookup(const AnserChannelKey *key, bool create);
static void anser_disp_apply_part(AnserDispChannel *chan,
								  const AnserPayloadOps *ops,
								  const void *payload, Size payload_len,
								  int total_parts, bool cancelled);
static void anser_disp_deliver(AnserDispChannel *chan);
static bool anser_disp_push(PGconn *conn, AnserDispChannel *chan);

/*
 * PQstatus() / PQerrorMessage() equivalents.  See the linkage note above for
 * why these are not the real thing; libpq-int.h gives us the full struct.
 */
static inline bool
anser_conn_ok(const PGconn *conn)
{
	return conn != NULL && conn->status == CONNECTION_OK;
}

static inline const char *
anser_conn_error(const PGconn *conn)
{
	if (conn == NULL)
		return "connection pointer is NULL";
	if (PQExpBufferBroken(&conn->errorMessage))
		return "out of memory";
	return conn->errorMessage.data;
}

/*
 * Per-query state lives in its own context so it can be dropped wholesale.
 * Channels are keyed by (session, command, condition), so entries from an
 * earlier command in the same transaction are distinct and simply unused
 * until the reset.
 */
static void
anser_disp_init(void)
{
	HASHCTL		hctl;

	if (AnserDispChannels != NULL)
		return;

	AnserDispContext = AllocSetContextCreate(TopMemoryContext,
											 "Anser dispatch transport",
											 ALLOCSET_DEFAULT_SIZES);

	MemSet(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(AnserChannelKey);
	hctl.entrysize = sizeof(AnserDispChannel);
	hctl.hcxt = AnserDispContext;

	AnserDispChannels = hash_create("Anser dispatch channels", 32, &hctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

void
AnserDispatchReset(void)
{
	if (AnserDispChannels == NULL)
		return;

	hash_destroy(AnserDispChannels);
	AnserDispChannels = NULL;
	MemoryContextDelete(AnserDispContext);
	AnserDispContext = NULL;
}

static AnserDispChannel *
anser_disp_lookup(const AnserChannelKey *key, bool create)
{
	AnserDispChannel *chan;
	bool		found;

	anser_disp_init();

	chan = (AnserDispChannel *) hash_search(AnserDispChannels, key,
											create ? HASH_ENTER : HASH_FIND,
											&found);
	if (chan == NULL)
		return NULL;

	if (create && !found)
	{
		/* hash_search only fills the key; initialize the rest. */
		chan->ops = NULL;
		chan->payload = NULL;
		chan->payload_len = 0;
		chan->parts_received = 0;
		chan->expected_parts = 0;
		chan->cancelled = false;
		chan->complete = false;
		chan->subscribers = NIL;
	}

	return chan;
}

/*
 * cdbdisp_notify_hook: is this one of ours, and if so, handle it.
 *
 * Returns false for any notify on another channel so the dispatcher can carry
 * on with its own handling.  A malformed payload is consumed (it is addressed
 * to us) but otherwise ignored: the affected consumers will time out and run
 * unfiltered.
 */
bool
AnserDispatchNotifyHandler(struct CdbDispatchResult *dispatchResult,
						   struct pgNotify *notify)
{
	PGnotify   *n = (PGnotify *) notify;
	CdbDispatchResult *dr = (CdbDispatchResult *) dispatchResult;
	int			sender = (dr != NULL && dr->segdbDesc != NULL)
		? dr->segdbDesc->segindex : -99;
	AnserWireMsg msg;
	const AnserPayloadOps *ops;
	AnserDispChannel *chan;
	MemoryContext oldcxt;

	if (n == NULL || n->relname == NULL ||
		strcmp(n->relname, ANSER_NOTIFY_CHANNEL) != 0)
		return false;

	if (n->extra == NULL || !AnserWireParse(n->extra, &msg))
	{
		elog(LOG, "anser: ignoring malformed message from a segment (len=%zu)",
			 n->extra != NULL ? strlen(n->extra) : (size_t) 0);
		return true;
	}

	/*
	 * Resolve what the message carries before doing anything with it.  An
	 * unregistered type means a producer was taught to send something this
	 * coordinator does not know how to merge -- almost certainly a payload type
	 * added to anserpayload.h but not to AnserPayloadTable.
	 */
	ops = AnserPayloadLookup(msg.payload_type);
	if (ops == NULL)
	{
		elog(LOG, "anser: unknown payload type '%c' from seg%d (cond=%u); ignoring",
			 msg.payload_type, sender, msg.key.condition_id);
		return true;
	}

	/*
	 * A message with no body can be checked here, before anything is
	 * allocated; one with a body is checked in the PART branch below, once it
	 * has been decoded.
	 */
	if (msg.body_len == 0 && !AnserWireCheckCrc(&msg, NULL, 0))
	{
		elog(LOG, "anser: ignoring corrupted message from seg%d (cond=%u kind=%c)",
			 sender, msg.key.condition_id, msg.kind);
		return true;
	}

	anser_disp_init();
	oldcxt = MemoryContextSwitchTo(AnserDispContext);

	chan = anser_disp_lookup(&msg.key, true);
	if (chan == NULL)
	{
		MemoryContextSwitchTo(oldcxt);
		return true;
	}

	if (msg.kind == ANSER_WIRE_KIND_SUBSCRIBE)
	{
		PGconn	   *conn = dr->segdbDesc->conn;

		/*
		 * A consumer can subscribe after the channel is already complete --
		 * producers on other segments may well have finished first -- so
		 * deliver immediately in that case rather than recording interest.
		 */
		ANSER_DEBUG("anser: QD subscribe cond=%u from seg%d (channel %s)",
					msg.key.condition_id, sender,
					chan->complete ? "complete, delivering now" : "still collecting");
		if (chan->complete)
			(void) anser_disp_push(conn, chan);
		else
			chan->subscribers = lappend(chan->subscribers, conn);
	}
	else if (msg.kind == ANSER_WIRE_KIND_PART)
	{
		char	   *raw = NULL;
		int			raw_len = 0;

		if (!(msg.flags & ANSER_WIRE_F_CANCELLED) && msg.body_len > 0)
		{
			int			maxlen = pg_b64_dec_len(msg.body_len);

			raw = palloc(maxlen);
			raw_len = pg_b64_decode(msg.body, msg.body_len, raw, maxlen);

			/*
			 * Undecodable, oversized or corrupted: cancel the channel rather
			 * than guess.  Consumers then run unfiltered, which is slower but
			 * correct -- folding in a part we cannot vouch for risks clearing a
			 * bit that should be set, and a filter missing a key silently drops
			 * joinable rows.
			 */
			if (raw_len < 0 || raw_len > gp_anser_max_info_size ||
				!AnserWireCheckCrc(&msg, raw, (Size) raw_len))
			{
				elog(LOG, "anser: unusable part for condition %u from seg%d; cancelling channel",
					 msg.key.condition_id, sender);
				pfree(raw);
				raw = NULL;
				raw_len = 0;
				msg.flags |= ANSER_WIRE_F_CANCELLED;
			}
		}

		anser_disp_apply_part(chan, ops, raw, (Size) raw_len, msg.total_parts,
							  (msg.flags & ANSER_WIRE_F_CANCELLED) != 0);
		ANSER_DEBUG("anser: QD %s part cond=%u from seg%d (says part %d of %d) %d/%d bytes=%d -> %s",
					ops->name, msg.key.condition_id, sender, msg.part_index,
					msg.total_parts, chan->parts_received,
					chan->expected_parts, raw_len,
					chan->cancelled ? "cancelled" :
					chan->complete ? "complete" : "collecting");
		if (raw != NULL)
			pfree(raw);
	}

	if (chan->complete)
		anser_disp_deliver(chan);

	MemoryContextSwitchTo(oldcxt);
	return true;
}

/*
 * Fold one part into the channel's accumulator.
 *
 * The first part is kept verbatim and becomes the accumulator; later parts are
 * merged into it in place by the payload type's fold(), so no part is ever
 * copied twice and the accumulator is never reallocated.  Nothing here knows
 * what the bytes are -- for a bloom filter the fold is a bitwise OR, for a row
 * count it would be a sum, and this function reads the same either way.
 */
static void
anser_disp_apply_part(AnserDispChannel *chan, const AnserPayloadOps *ops,
					  const void *payload, Size payload_len, int total_parts,
					  bool cancelled)
{
	if (chan->cancelled)
		return;					/* already dead; nothing to do */

	if (chan->ops == NULL)
		chan->ops = ops;
	else if (chan->ops != ops)
	{
		/*
		 * Two producers disagree about what this channel carries.  Merging
		 * across types is meaningless, so give up on the channel: consumers run
		 * unfiltered, which is always correct.
		 */
		elog(LOG, "anser: cond=%u carries both '%s' and '%s'; cancelling channel",
			 chan->key.condition_id, chan->ops->name, ops->name);
		chan->cancelled = true;
		chan->complete = true;
		chan->payload = NULL;
		chan->payload_len = 0;
		return;
	}

	if (total_parts > chan->expected_parts)
	{
		/*
		 * Take the largest count any producer claims.  Letting a later part
		 * lower it would complete the channel early and deliver a filter
		 * missing another segment's keys -- a false negative, which drops
		 * joinable rows.  Disagreement means the producers computed their
		 * slice width differently and is worth seeing.
		 */
		if (chan->expected_parts != 0)
			elog(LOG, "anser: cond=%u producer count changed %d -> %d",
				 chan->key.condition_id, chan->expected_parts, total_parts);
		chan->expected_parts = total_parts;
	}

	if (cancelled)
	{
		chan->cancelled = true;
		chan->complete = true;
		chan->payload = NULL;
		chan->payload_len = 0;
		return;
	}

	if (payload == NULL || payload_len == 0)
	{
		/* An empty part still counts toward completion. */
		chan->parts_received++;
	}
	else if (chan->payload == NULL)
	{
		chan->payload = palloc(payload_len);
		memcpy(chan->payload, payload, payload_len);
		chan->payload_len = payload_len;
		chan->parts_received++;
	}
	else if (ops->fold != NULL &&
			 ops->fold(chan->payload, chan->payload_len, payload, payload_len))
	{
		chan->parts_received++;
	}
	else
	{
		/*
		 * The parts cannot be merged -- for a bloom filter, sizes or filter
		 * parameters disagree.  That should not happen (every part on a channel
		 * is built from the same plan parameters), but if it does the only safe
		 * answer is to give up on the channel.  A type with no fold() reaches
		 * here too, which is right: it should never have carried a body.
		 */
		elog(LOG, "anser: incompatible '%s' part for condition %u; cancelling channel",
			 ops->name, chan->key.condition_id);
		chan->cancelled = true;
		chan->complete = true;
		chan->payload = NULL;
		chan->payload_len = 0;
		return;
	}

	if (chan->expected_parts > 0 && chan->parts_received >= chan->expected_parts)
	{
		chan->complete = true;

		/*
		 * Every part is in, so this is the first and last chance to judge the
		 * merged result.  A type that declines it here saves each consumer both
		 * the delivery and the per-row probing it would have paid for.
		 */
		if (chan->payload != NULL && ops->worth_delivering != NULL &&
			!ops->worth_delivering(chan->payload, chan->payload_len))
		{
			elog(LOG, "anser: merged '%s' payload for condition %u is not worth delivering; cancelling channel",
				 ops->name, chan->key.condition_id);
			chan->cancelled = true;
			chan->payload = NULL;
			chan->payload_len = 0;
		}
	}
}

/* Push the finished channel to everyone waiting, then forget them. */
static void
anser_disp_deliver(AnserDispChannel *chan)
{
	ListCell   *lc;

	ANSER_DEBUG("anser: QD delivering cond=%u to %d subscriber(s)",
				chan->key.condition_id, list_length(chan->subscribers));
	foreach(lc, chan->subscribers)
		(void) anser_disp_push((PGconn *) lfirst(lc), chan);

	list_free(chan->subscribers);
	chan->subscribers = NIL;
}

/*
 * Write one merged payload to a QE as a GP_SIDEBAND_MESSAGE.
 *
 * Delivery is per consumer: a write that fails costs that one segment its
 * filter (it will time out and run unfiltered) and leaves the others alone.
 * This is the same "try to reach every consumer" rule the shared-memory send
 * service followed.
 */
static bool
anser_disp_push(PGconn *conn, AnserDispChannel *chan)
{
	int			flags = chan->cancelled ? ANSER_WIRE_F_CANCELLED : 0;
	int			keylen = (int) strlen(chan->key.condition_key);
	int			paylen = chan->cancelled ? 0 : (int) chan->payload_len;
	pg_crc32c	crc;
	char		paytype;

	if (!anser_conn_ok(conn))
		return false;

	/*
	 * A channel that was cancelled before any part arrived has no type.  That
	 * is fine: the delivery carries no body, and a consumer only checks the
	 * type of a body it actually received.
	 */
	paytype = chan->ops != NULL ? chan->ops->code : ANSER_PAYLOAD_NONE;

	crc = AnserWirePushCrc(paytype, chan->key.condition_id, (uint32) flags,
							   chan->key.condition_key, keylen,
							   chan->payload, paylen);

	/*
	 * Raw binary: pqPutnchar performs no encoding conversion, so unlike the
	 * QE -> QD direction this needs no base64.
	 */
	if (pqPutMsgStart(GP_SIDEBAND_MESSAGE, conn) < 0 ||
		pqPutInt((int) paytype, 4, conn) < 0 ||
		pqPutInt((int) crc, 4, conn) < 0 ||
		pqPutInt((int) chan->key.condition_id, 4, conn) < 0 ||
		pqPutInt(flags, 4, conn) < 0 ||
		pqPutInt(keylen, 4, conn) < 0 ||
		pqPutnchar(chan->key.condition_key, keylen, conn) < 0 ||
		pqPutInt(paylen, 4, conn) < 0 ||
		(paylen > 0 && pqPutnchar(chan->payload, paylen, conn) < 0) ||
		pqPutMsgEnd(conn) < 0 ||
		pqFlush(conn) < 0)
	{
		elog(LOG, "anser: could not deliver filter for condition %u: %s",
			 chan->key.condition_id, anser_conn_error(conn));
		return false;
	}

	ANSER_DEBUG("anser: QD pushed cond=%u type=%c bytes=%d cancelled=%d",
				chan->key.condition_id, paytype, paylen,
				chan->cancelled ? 1 : 0);
	return true;
}

/*
 * Parse a QE -> QD payload: fixed-width header, then the condition key, then
 * the base64 body.  ansersideband.h documents the layout and the reasoning; the
 * checks here are what make it trustworthy:
 *
 *   - the tag must match, so a message from a different format is not
 *     misinterpreted as this one;
 *   - the header is a constant length, so the key and the body start at offsets
 *     that no payload byte can influence;
 *   - the two lengths must account for the message exactly, which catches a
 *     truncated or over-long message before any of it is used;
 *   - the CRC is verified once the body has been decoded, in the caller.
 *
 * The payload type is carried through as the raw byte; resolving it against the
 * registry is the caller's job, so that an unregistered type is reported as
 * exactly that rather than as a framing error.
 */
bool
AnserWireParse(const char *msg, AnserWireMsg *out)
{
	Size		msglen = strlen(msg);
	char		kind;
	char		paytype;
	uint32		ssid,
				ccnt,
				condid,
				part,
				total,
				flags,
				keylen,
				bodylen,
				crc;

	if (msglen < ANSER_WIRE_HDR_LEN)
		return false;
	if (strncmp(msg, ANSER_WIRE_TAG " ", sizeof(ANSER_WIRE_TAG)) != 0)
		return false;

	if (sscanf(msg, ANSER_WIRE_HDR_SCANF,
			   &kind, &paytype, &ssid, &ccnt, &condid, &part, &total, &flags,
			   &keylen, &bodylen, &crc) != 11)
		return false;

	if (kind != ANSER_WIRE_KIND_PART && kind != ANSER_WIRE_KIND_SUBSCRIBE)
		return false;
	if (keylen >= ANSER_CONDITION_KEY_SIZE)
		return false;
	if (msglen != ANSER_WIRE_HDR_LEN + (Size) keylen + (Size) bodylen)
		return false;

	MemSet(out, 0, sizeof(*out));
	out->wire = msg;
	out->kind = kind;
	out->payload_type = paytype;
	out->key.gp_session_id = (int) ssid;
	out->key.gp_command_count = (int) ccnt;
	out->key.condition_id = condid;
	memcpy(out->key.condition_key, msg + ANSER_WIRE_HDR_LEN, keylen);
	out->key.condition_key[keylen] = '\0';
	out->key_len = (int) keylen;
	out->part_index = (int) part;
	out->total_parts = (int) total;
	out->flags = (int) flags;
	out->body = msg + ANSER_WIRE_HDR_LEN + keylen;
	out->body_len = (int) bodylen;
	out->crc = crc;

	return true;
}

/*
 * Verify a parsed message against its checksum.
 *
 * 'body' is the decoded body, which is what the producer checksummed -- so
 * where it is covered this validates the base64 round trip as well.  Pass
 * NULL/0 for a message that has no body, and note that a type which opts out of
 * checksumming its body still has its header and key checked.
 */
bool
AnserWireCheckCrc(const AnserWireMsg *msg, const void *body, Size body_len)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, msg->wire, ANSER_WIRE_CRC_OFFSET);
	COMP_CRC32C(crc, msg->key.condition_key, msg->key_len);
	if (body != NULL && body_len > 0 &&
		AnserPayloadChecksumsBody(msg->payload_type))
		COMP_CRC32C(crc, body, body_len);
	FIN_CRC32C(crc);

	return (uint32) crc == msg->crc;
}

/*
 * Coordinator-local producer.
 *
 * A producer running on the QD has no dispatch connection to itself, so it
 * folds straight into the same channel table the hook uses.
 */
bool
AnserDispatchLocalPublish(const AnserChannelKey *channel_key, char payload_type,
						  uint32 part_index, uint32 total_parts,
						  const void *payload, Size payload_len,
						  bool cancelled)
{
	const AnserPayloadOps *ops;
	AnserDispChannel *chan;
	MemoryContext oldcxt;

	if (channel_key == NULL)
		return false;
	if (!cancelled && payload_len > (Size) gp_anser_max_info_size)
		cancelled = true;

	ops = AnserPayloadLookup(payload_type);
	if (ops == NULL)
	{
		elog(LOG, "anser: unknown payload type '%c' for condition %u; ignoring",
			 payload_type, channel_key->condition_id);
		return false;
	}

	anser_disp_init();
	oldcxt = MemoryContextSwitchTo(AnserDispContext);

	chan = anser_disp_lookup(channel_key, true);
	if (chan != NULL)
	{
		anser_disp_apply_part(chan, ops, payload, payload_len,
							  (int) total_parts, cancelled);
		ANSER_DEBUG("anser: QD local %s part cond=%u (part %u of %u) %d/%d bytes=%zu -> %s",
					ops->name, channel_key->condition_id, part_index, total_parts,
					chan->parts_received, chan->expected_parts, payload_len,
					chan->cancelled ? "cancelled" :
					chan->complete ? "complete" : "collecting");
		if (chan->complete)
			anser_disp_deliver(chan);
	}

	MemoryContextSwitchTo(oldcxt);
	return chan != NULL;
}

/*
 * Coordinator-local consumer.
 *
 * Does not wait: on the coordinator the producer side of the join has already
 * run by the time the probe side asks for the filter, so either the channel is
 * complete or it never will be (a squelched producer, say) and we fail open.
 */
bool
AnserDispatchLocalConsume(const AnserChannelKey *channel_key, char payload_type,
						  void **payload, Size *payload_len, bool *cancelled)
{
	AnserDispChannel *chan;

	if (payload != NULL)
		*payload = NULL;
	if (payload_len != NULL)
		*payload_len = 0;
	if (cancelled != NULL)
		*cancelled = false;

	if (channel_key == NULL || AnserDispChannels == NULL)
		return false;

	chan = anser_disp_lookup(channel_key, false);
	if (chan == NULL || !chan->complete)
		return false;

	if (chan->cancelled || chan->payload == NULL)
	{
		if (cancelled != NULL)
			*cancelled = true;
		return false;
	}

	/* Same check the segment path makes on delivery; see anser_inbox_take. */
	if (chan->ops == NULL || chan->ops->code != payload_type)
	{
		elog(WARNING, "anser: condition %u holds payload type '%c', expected '%c'",
			 channel_key->condition_id,
			 chan->ops != NULL ? chan->ops->code : '?', payload_type);
		if (cancelled != NULL)
			*cancelled = true;
		return false;
	}

	if (payload != NULL)
	{
		*payload = palloc(chan->payload_len);
		memcpy(*payload, chan->payload, chan->payload_len);
	}
	if (payload_len != NULL)
		*payload_len = chan->payload_len;

	return true;
}
