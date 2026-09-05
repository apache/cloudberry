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
 * IDENTIFICATION
 *	  gpcontrib/anser/src/anserdispatch.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq-fe.h"
#include "libpq-int.h"

#include "anser.h"
#include "anserfilter.h"
#include "ansersideband.h"
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
	char	   *payload;		/* merged part, or NULL before the first one */
	Size		payload_len;
	int			parts_received;
	int			expected_parts;	/* 0 until a part tells us; from total_parts */
	bool		cancelled;
	bool		complete;		/* every expected part folded, or cancelled */
	List	   *subscribers;	/* PGconn * of QEs awaiting delivery */
} AnserDispChannel;

/* Parsed QE -> QD message. */
typedef struct AnserWireMsg
{
	char		kind;
	AnserChannelKey key;
	int			part_index;
	int			total_parts;
	int			flags;
	const char *body;			/* base64, not NUL-terminated */
	int			body_len;
} AnserWireMsg;

static HTAB *AnserDispChannels = NULL;
static MemoryContext AnserDispContext = NULL;

static AnserDispChannel *anser_disp_lookup(const AnserChannelKey *key, bool create);
static bool anser_disp_parse(const char *msg, AnserWireMsg *out);
static void anser_disp_apply_part(AnserDispChannel *chan, const void *payload,
								  Size payload_len, int total_parts, bool cancelled);
static void anser_disp_deliver(AnserDispChannel *chan);
static bool anser_disp_push(PGconn *conn, AnserDispChannel *chan);

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
	AnserWireMsg msg;
	AnserDispChannel *chan;
	MemoryContext oldcxt;

	if (n == NULL || n->relname == NULL ||
		strcmp(n->relname, ANSER_NOTIFY_CHANNEL) != 0)
		return false;

	if (n->extra == NULL || !anser_disp_parse(n->extra, &msg))
	{
		elog(LOG, "anser: ignoring malformed message from a segment");
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
		PGconn	   *conn = ((CdbDispatchResult *) dispatchResult)->segdbDesc->conn;

		/*
		 * A consumer can subscribe after the channel is already complete --
		 * producers on other segments may well have finished first -- so
		 * deliver immediately in that case rather than recording interest.
		 */
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
			if (raw_len < 0 || raw_len > gp_anser_max_info_size)
			{
				/* Undecodable or oversized: cancel rather than guess. */
				pfree(raw);
				raw = NULL;
				raw_len = 0;
				msg.flags |= ANSER_WIRE_F_CANCELLED;
			}
		}

		anser_disp_apply_part(chan, raw, (Size) raw_len, msg.total_parts,
							  (msg.flags & ANSER_WIRE_F_CANCELLED) != 0);
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
 * OR'd into it in place (AnserBloomFoldPartInPlace), so no part is ever copied
 * twice and the accumulator is never reallocated.
 */
static void
anser_disp_apply_part(AnserDispChannel *chan, const void *payload,
					  Size payload_len, int total_parts, bool cancelled)
{
	if (chan->cancelled)
		return;					/* already dead; nothing to do */

	if (total_parts > 0 && chan->expected_parts == 0)
		chan->expected_parts = total_parts;

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
	else if (AnserBloomFoldPartInPlace(chan->payload, chan->payload_len,
									   payload, payload_len))
	{
		chan->parts_received++;
	}
	else
	{
		/*
		 * Sizes or parameters disagree, so the parts cannot be unioned.  That
		 * should not happen (every part on a channel is built from the same
		 * plan parameters), but if it does the only safe answer is to give up
		 * on the channel.
		 */
		elog(LOG, "anser: incompatible part for condition %u; cancelling channel",
			 chan->key.condition_id);
		chan->cancelled = true;
		chan->complete = true;
		chan->payload = NULL;
		chan->payload_len = 0;
		return;
	}

	if (chan->expected_parts > 0 && chan->parts_received >= chan->expected_parts)
		chan->complete = true;
}

/* Push the finished channel to everyone waiting, then forget them. */
static void
anser_disp_deliver(AnserDispChannel *chan)
{
	ListCell   *lc;

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

	if (conn == NULL || PQstatus(conn) != CONNECTION_OK)
		return false;

	/*
	 * Raw binary: pqPutnchar performs no encoding conversion, so unlike the
	 * QE -> QD direction this needs no base64.
	 */
	if (pqPutMsgStart(GP_SIDEBAND_MESSAGE, conn) < 0 ||
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
			 chan->key.condition_id, PQerrorMessage(conn));
		return false;
	}

	return true;
}

/*
 * Parse a QE -> QD payload.
 *
 * Layout: a single-line text header, then the condition key, then the body.
 *
 *   anser1 <kind> <ssid> <ccnt> <condid> <part> <total> <flags> <keylen> <bodylen>\n
 *   <condition_key><body>
 *
 * The header holds only numbers and one character, so it cannot contain the
 * newline that terminates it; key and body are taken by length, so neither
 * needs escaping or a delimiter of its own.
 */
static bool
anser_disp_parse(const char *msg, AnserWireMsg *out)
{
	const char *nl;
	const char *rest;
	char		kind;
	int			ssid,
				ccnt,
				condid,
				part,
				total,
				flags,
				keylen,
				bodylen;

	nl = strchr(msg, '\n');
	if (nl == NULL)
		return false;

	if (sscanf(msg, ANSER_WIRE_TAG " %c %d %d %d %d %d %d %d %d",
			   &kind, &ssid, &ccnt, &condid, &part, &total, &flags,
			   &keylen, &bodylen) != 9)
		return false;

	if (kind != ANSER_WIRE_KIND_PART && kind != ANSER_WIRE_KIND_SUBSCRIBE)
		return false;
	if (condid < 0 || keylen < 0 || bodylen < 0 ||
		keylen >= ANSER_CONDITION_KEY_SIZE)
		return false;

	rest = nl + 1;
	if ((int) strlen(rest) != keylen + bodylen)
		return false;

	MemSet(out, 0, sizeof(*out));
	out->kind = kind;
	out->key.gp_session_id = ssid;
	out->key.gp_command_count = ccnt;
	out->key.condition_id = (uint32) condid;
	memcpy(out->key.condition_key, rest, keylen);
	out->key.condition_key[keylen] = '\0';
	out->part_index = part;
	out->total_parts = total;
	out->flags = flags;
	out->body = rest + keylen;
	out->body_len = bodylen;

	return true;
}

/*
 * Coordinator-local producer.
 *
 * A producer running on the QD has no dispatch connection to itself, so it
 * folds straight into the same channel table the hook uses.
 */
bool
AnserDispatchLocalPublish(const AnserChannelKey *channel_key,
						  uint32 part_index, uint32 total_parts,
						  const void *payload, Size payload_len,
						  bool cancelled)
{
	AnserDispChannel *chan;
	MemoryContext oldcxt;

	if (channel_key == NULL)
		return false;
	if (!cancelled && payload_len > (Size) gp_anser_max_info_size)
		cancelled = true;

	anser_disp_init();
	oldcxt = MemoryContextSwitchTo(AnserDispContext);

	chan = anser_disp_lookup(channel_key, true);
	if (chan != NULL)
	{
		anser_disp_apply_part(chan, payload, payload_len, (int) total_parts,
							  cancelled);
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
AnserDispatchLocalConsume(const AnserChannelKey *channel_key,
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

	if (payload != NULL)
	{
		*payload = palloc(chan->payload_len);
		memcpy(*payload, chan->payload, chan->payload_len);
	}
	if (payload_len != NULL)
		*payload_len = chan->payload_len;

	return true;
}
