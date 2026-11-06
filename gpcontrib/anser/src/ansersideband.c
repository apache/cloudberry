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
 * ansersideband.c
 *	  Segment side of the dispatch-connection transport.
 *
 * A producer sends its part as a NOTIFY and moves on; a consumer subscribes,
 * then blocks on its own dispatch socket until the coordinator pushes the
 * merged filter back.  Both directions reuse the connection the dispatcher
 * already owns, so there is no second connection to open and nothing to
 * authenticate.
 *
 * Reading the frontend socket in the middle of executing a query is the
 * pattern cdb_sequence_nextval_qe() established (commands/sequence.c); the
 * loop below mirrors its use of pq_startmsgread/pq_getbyte_if_available, but
 * sleeps on the socket instead of spinning.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/src/ansersideband.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "anser.h"
#include "ansersideband.h"
#include "cdb/cdbvars.h"
#include "commands/async.h"
#include "common/base64.h"
#include "libpq/libpq-be.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "port/pg_bswap.h"
#include "port/pg_crc32c.h"
#include "portability/instr_time.h"
#include "storage/latch.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "utils/faultinjector.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

/* How long to sleep between wakeups while waiting for a delivery. */
#define ANSER_SIDEBAND_POLL_MS	100L

/*
 * Delivery state: one live subscription, one assembly slot.
 *
 * With pull-based chunking there is at most one live subscription per
 * process: AnserSidebandConsumeWait() is called from one place
 * (anserbloomconsume.c), its loop never runs the executor, so waits cannot
 * nest, and every exit path clears the flag.  And nothing arrives without a
 * subscription: the coordinator sends a chunk only after a SUBSCRIBE or a
 * NEXT.  So the push model's inbox -- several parked deliveries, one per
 * channel -- collapses to a single slot, and the cases "several entries" and
 * "an entry for someone else" are impossible by construction rather than by
 * argument.
 *
 * The full-key match is still what rejects a late chunk from an earlier
 * command: AnserWaitKey carries this command's session and command ids, so a
 * chunk from an older one never enters the slot.
 */
typedef struct AnserInboxEntry
{
	AnserChannelKey key;		/* for the trace lines; always AnserWaitKey */
	char		payload_type;
	char	   *payload;		/* the assembly buffer; NULL when cancelled */
	Size		total_len;		/* declared full size; 0 when cancelled */
	Size		received;		/* bytes assembled so far */
	instr_time	last_chunk_at;	/* when the latest chunk was accepted */
	bool		complete;		/* received == total_len, or cancelled */
	bool		cancelled;
} AnserInboxEntry;

static bool AnserWaiting = false;	/* a subscription is live... */
static AnserChannelKey AnserWaitKey;	/* ...for this key */
static AnserInboxEntry *AnserSlot = NULL;	/* its assembly, or NULL */

static bool anser_sideband_send(const char *payload);
static bool anser_inbox_take(const AnserChannelKey *key, char payload_type,
							 void **payload, Size *payload_len, bool *cancelled);
static bool anser_sideband_read_one(long timeout_ms);
static bool anser_key_equal(const AnserChannelKey *a, const AnserChannelKey *b);
static void anser_slot_free(void);

static bool
anser_key_equal(const AnserChannelKey *a, const AnserChannelKey *b)
{
	return a->condition_id == b->condition_id &&
		a->gp_session_id == b->gp_session_id &&
		a->gp_command_count == b->gp_command_count &&
		strncmp(a->condition_key, b->condition_key,
				ANSER_CONDITION_KEY_SIZE) == 0;
}

/* Free the assembly slot, whatever state the delivery is in. */
static void
anser_slot_free(void)
{
	if (AnserSlot != NULL)
	{
		ANSER_DEBUG("anser: seg%d dropping delivery cond=%u pushed for ssid=%d ccnt=%d (%zu of %zu bytes, complete=%d)",
					GpIdentity.segindex, AnserSlot->key.condition_id,
					AnserSlot->key.gp_session_id, AnserSlot->key.gp_command_count,
					AnserSlot->received, AnserSlot->total_len,
					AnserSlot->complete ? 1 : 0);
		if (AnserSlot->payload != NULL)
			pfree(AnserSlot->payload);
		pfree(AnserSlot);
		AnserSlot = NULL;
	}
}

/*
 * Publish one part.  Fire-and-forget: the coordinator does not acknowledge,
 * because nothing on this side needs to wait for it.
 */
bool
AnserSidebandPublish(const AnserChannelKey *channel_key, char payload_type,
					 uint32 part_index, uint32 total_parts,
					 const void *payload, Size payload_len, bool cancelled)
{
	char	   *msg;
	int			flags = cancelled ? ANSER_WIRE_F_CANCELLED : 0;
	bool		ok;

	if (channel_key == NULL)
		return false;

	if (!cancelled && payload_len > (Size) gp_anser_max_info_size)
	{
		/* Too large to ship; tell the coordinator so consumers stop waiting. */
		flags = ANSER_WIRE_F_CANCELLED;
		payload = NULL;
		payload_len = 0;
	}

	msg = AnserWireFormat(channel_key, ANSER_WIRE_KIND_PART, payload_type,
						  part_index, total_parts, flags,
						  (flags & ANSER_WIRE_F_CANCELLED) ? NULL : payload,
						  (flags & ANSER_WIRE_F_CANCELLED) ? 0 : payload_len);
	ok = anser_sideband_send(msg);
	ANSER_DEBUG("anser: seg%d published cond=%u type=%c part=%u/%u bytes=%zu cancelled=%d sent=%d",
				GpIdentity.segindex, channel_key->condition_id, payload_type,
				part_index, total_parts, payload_len,
				(flags & ANSER_WIRE_F_CANCELLED) ? 1 : 0, ok ? 1 : 0);
	pfree(msg);

	return ok;
}

/*
 * Withdraw a subscription, in full: tell the coordinator, forget our own
 * record of it, and drop the delivery slot whatever state it is in -- a
 * completed but unclaimed payload is just as dead once its consumer walks
 * away.  Called on every path out of AnserSidebandConsumeWait that leaves a
 * subscription behind (deadline expiry, query finishing early).
 *
 * With chunked delivery this closes the problem rather than narrowing a
 * window: the coordinator stops at the chunk boundary, at most one whole
 * chunk can still be in flight, and read_one drops it because the slot has no
 * live subscription -- or, if we have already left the wait, the command loop
 * skips the message whole (postgres.c, GP_SIDEBAND_MESSAGE).  A send failure
 * is only logged: the coordinator then keeps a stalled delivery until the
 * channel is reset, which costs it nothing further.
 */
static void
anser_sideband_withdraw(const AnserChannelKey *channel_key)
{
	char	   *msg = AnserWireFormat(channel_key, ANSER_WIRE_KIND_UNSUBSCRIBE,
									ANSER_PAYLOAD_NONE, 0, 0, 0, NULL, 0);

	if (!anser_sideband_send(msg))
		ANSER_DEBUG("anser: seg%d could not unsubscribe from cond=%u",
					GpIdentity.segindex, channel_key->condition_id);
	pfree(msg);

	AnserWaiting = false;
	anser_slot_free();
}

/*
 * Subscribe, then wait for the merged payload.
 *
 * Returns true with *payload set, or false for "run unfiltered" -- including
 * on timeout.  The deadline exists because a producer that gets squelched
 * never publishes anything: ExecSquelchNode does not call CustomScan
 * callbacks, it only marks the node (execAmi.c), so without a deadline this
 * wait could outlive the reason for it.
 */
bool
AnserSidebandConsumeWait(const AnserChannelKey *channel_key, char payload_type,
						 void **payload, Size *payload_len,
						 bool *cancelled, long timeout_ms)
{
	char	   *msg;
	instr_time	subscribe_start;
	instr_time	now;

	if (payload != NULL)
		*payload = NULL;
	if (payload_len != NULL)
		*payload_len = 0;
	if (cancelled != NULL)
		*cancelled = false;

	if (channel_key == NULL || MyProcPort == NULL ||
		MyProcPort->sock == PGINVALID_SOCKET)
	{
		/*
		 * No way to reach the coordinator from here, so this consumer will run
		 * unfiltered without ever having asked for anything.  Which of the
		 * three reasons it was matters: a process with no Port is one the
		 * sideband was never designed for, while a bad socket on a process
		 * that has one is a different problem entirely.
		 */
		ANSER_DEBUG("anser: seg%d cannot consume cond=%u: key=%s port=%s sock=%d role=%d writer=%d dest=%d",
					GpIdentity.segindex,
					channel_key != NULL ? channel_key->condition_id : 0,
					channel_key != NULL ? "ok" : "NULL",
					MyProcPort != NULL ? "ok" : "NULL",
					MyProcPort != NULL ? (int) MyProcPort->sock : -1,
					(int) Gp_role, Gp_is_writer ? 1 : 0,
					(int) whereToSendOutput);
		return false;
	}

	/*
	 * With no deadline to wait out there is nothing to subscribe for: we would
	 * give up on the next line anyway, and the only lasting effect would be a
	 * stalled delivery on the coordinator.  So do not ask in the first place.
	 */
	if (timeout_ms == 0)
	{
		ANSER_DEBUG("anser: seg%d not subscribing to cond=%u: no deadline to wait out",
					GpIdentity.segindex, channel_key->condition_id);
		return false;
	}

	/*
	 * One subscription at a time, by construction -- see the slot's comment.
	 * A slot left over from an error exit (PG error unwinding passes none of
	 * the exits below) is always stale: its command count is not ours.
	 */
	Assert(!AnserWaiting);
	if (AnserSlot != NULL)
		anser_slot_free();

	/*
	 * A subscription carries nothing, so its payload type is NONE -- what this
	 * consumer expects to receive is checked on delivery, not announced here.
	 * The key is marked live before anything can arrive: read_one drops chunks
	 * unless the subscription is, which is how a delivery whose consumer has
	 * given up ends at its current chunk.
	 */
	msg = AnserWireFormat(channel_key, ANSER_WIRE_KIND_SUBSCRIBE,
						  ANSER_PAYLOAD_NONE, 0, 0, 0, NULL, 0);
	if (!anser_sideband_send(msg))
	{
		ANSER_DEBUG("anser: seg%d could not subscribe to cond=%u (role=%d writer=%d dest=%d)",
					GpIdentity.segindex, channel_key->condition_id,
					(int) Gp_role, Gp_is_writer ? 1 : 0,
					(int) whereToSendOutput);
		pfree(msg);
		return false;
	}
	pfree(msg);
	AnserWaitKey = *channel_key;
	AnserWaiting = true;
	ANSER_DEBUG("anser: seg%d subscribed cond=%u, waiting up to %ld ms",
				GpIdentity.segindex, channel_key->condition_id, timeout_ms);

	INSTR_TIME_SET_CURRENT(subscribe_start);
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * The coordinator has what it needs and told us to wrap up -- a LIMIT
		 * that filled, typically.  CHECK_FOR_INTERRUPTS does not cover this:
		 * it is a flag the executor is expected to notice, which is why
		 * nodeHashjoin.c tests it beside the same call.  Without that, a
		 * finished query is held open by a consumer still waiting on a filter
		 * nobody will use, for up to anser.timeout_ms.
		 */
		if (QueryFinishPending)
		{
			ANSER_DEBUG("anser: seg%d abandoning cond=%u: the query is finishing",
						GpIdentity.segindex, channel_key->condition_id);

			/*
			 * Same reason the deadline path withdraws: the query is wrapping
			 * up, so nobody here will read the filter the coordinator is
			 * about to push.
			 */
			anser_sideband_withdraw(channel_key);
			return false;
		}

		if (anser_inbox_take(channel_key, payload_type, payload, payload_len,
							 cancelled))
		{
			AnserWaiting = false;
			return payload != NULL && *payload != NULL;
		}

		/*
		 * The deadline guards against a delivery that stalls: it counts from
		 * the last accepted chunk (or from the subscribe while none has
		 * arrived), not from the subscribe alone -- aborting a healthy
		 * delivery mid-flight for being large would only waste the work
		 * already done.  A delivery that trickles just under the deadline
		 * forever must still end, so a hard cap a few times the deadline
		 * counts from the subscribe.
		 */
		if (timeout_ms >= 0)
		{
			double		since_progress;
			double		since_subscribe;
			instr_time *last = (AnserSlot != NULL) ?
				&AnserSlot->last_chunk_at : &subscribe_start;

			INSTR_TIME_SET_CURRENT(now);
			INSTR_TIME_SUBTRACT(now, *last);
			since_progress = INSTR_TIME_GET_MILLISEC(now);

			INSTR_TIME_SET_CURRENT(now);
			INSTR_TIME_SUBTRACT(now, subscribe_start);
			since_subscribe = INSTR_TIME_GET_MILLISEC(now);

			if (since_progress >= timeout_ms ||
				since_subscribe >= (double) timeout_ms * 4)
			{
				/*
				 * Withdraw the subscription on the way out.  From here on
				 * this process is somewhere else in the plan, and the
				 * chunked delivery stops at its current chunk once the
				 * withdrawal lands.
				 *
				 * Withdraw frees the slot, so read the totals first: "gave up
				 * having received 0 of 0" and "never started" should not look
				 * alike in the log.
				 */
				Size		got = AnserSlot != NULL ? AnserSlot->received : 0;
				Size		total = AnserSlot != NULL ? AnserSlot->total_len : 0;

				anser_sideband_withdraw(channel_key);

				ANSER_DEBUG("anser: seg%d gave up on cond=%u after %ld ms (received %zu of %zu bytes)",
							GpIdentity.segindex, channel_key->condition_id,
							timeout_ms, got, total);
				return false;
			}
		}

		/*
		 * The chunk we read lands in the slot; the loop then rechecks whether
		 * the delivery is complete.  A read failure means the connection is
		 * gone, which the interconnect will report far more usefully than we
		 * can -- stop waiting and let the query run unfiltered.
		 */
		if (!anser_sideband_read_one(ANSER_SIDEBAND_POLL_MS))
		{
			anser_sideband_withdraw(channel_key);
			return false;
		}
	}
}

/*
 * Wait briefly for one sideband message and stash it in the inbox.
 *
 * Returns false only when the connection is unusable; a timeout with nothing
 * to read is a normal true.
 */
static bool
anser_sideband_read_one(long timeout_ms)
{
	unsigned char qtype;
	int			retval;
	int64		maxlen;
	StringInfoData buf;
	AnserChannelKey key;
	MemoryContext oldcxt;
	char		paytype;
	uint32		crc;
	int			condid;
	uint32		session_id;
	uint32		command_count;
	int			flags;
	uint32		total_len;
	uint32		offset;
	int			keylen;
	int			chunklen;
	const char *keyptr;
	const char *payptr;

	pq_startmsgread();
	retval = pq_getbyte_if_available(&qtype);
	if (retval == 0)
	{
		/* Nothing buffered: sleep on the socket rather than spinning. */
		pq_endmsgread();

		ResetLatch(MyLatch);
		(void) WaitLatchOrSocket(MyLatch,
								 WL_LATCH_SET | WL_SOCKET_READABLE |
								 WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
								 MyProcPort->sock,
								 timeout_ms,
								 PG_WAIT_EXTENSION);
		return true;
	}

	if (retval == EOF)
	{
		pq_endmsgread();
		elog(LOG, "anser: dispatch connection closed while awaiting a filter");
		return false;
	}

	if (qtype != GP_SIDEBAND_MESSAGE)
	{
		/*
		 * The one type that can legitimately turn up is '?', the nextval
		 * response: the QD sends it and cancels the QEs when it fails to get a
		 * value, so it can arrive with nobody waiting for it.  We do not skip
		 * it, because that would consume a reply a later nextval might be
		 * about to wait for, and we are not the owner of that exchange.  Other
		 * types should not reach us at all.
		 *
		 * Either way there is no way back by the time we are here:
		 * pq_getbyte_if_available() has already taken the byte, and the
		 * backend cannot push one in front of the stream, so the message can
		 * neither be handed to the command loop nor left for its owner.  The
		 * connection is one byte short from here on.
		 *
		 * Terminating is therefore the only honest outcome.  Returning and
		 * running unfiltered -- the instinct everywhere else in this code --
		 * would be much worse than losing the filter: every later read lands
		 * at the wrong offset, and the failure surfaces somewhere else
		 * entirely as an absurd allocation request or "invalid frontend
		 * message type".  It has to be FATAL rather than ERROR because the
		 * connection cannot be reused: an ERROR would return this QE to the
		 * gang pool with a desynchronised stream, poisoning a later query.
		 *
		 * The mirror case -- a sideband message reaching the nextval reader --
		 * is the common one, and that reader skips it rather than dying: it
		 * knows our message is unowned by then, which we cannot say of its.
		 */
		pq_endmsgread();
		ereport(FATAL,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("anser: unexpected message type '%c' while awaiting a filter",
						(char) qtype),
				 errdetail("The message boundary was lost; this connection cannot be used further.")));
	}

	/*
	 * In int64 and clamped, though anser.max_info_size is bounded so that it
	 * cannot overflow this on its own.  The bound lives in another file; this
	 * costs nothing and does not depend on the two staying in step.
	 */
	maxlen = (int64) gp_anser_max_info_size + ANSER_SIDEBAND_FRAME_ROOM;
	if (maxlen > (int64) PQ_LARGE_MESSAGE_LIMIT)
		maxlen = (int64) PQ_LARGE_MESSAGE_LIMIT;

	initStringInfo(&buf);
	if (pq_getmessage(&buf, (int) maxlen) != 0)
	{
		/*
		 * Same reasoning as the message type above, and the same outcome.
		 * Every path on which pq_getmessage returns EOF leaves us unable to go
		 * on: either the peer is gone, or -- when the length word exceeds the
		 * cap -- it has taken those four bytes and left the body in the stream
		 * (pqcomm.c discards the body only on the paths where it already read
		 * it).  Running unfiltered from here would hand the gang pool a
		 * connection whose next read lands mid-message.
		 *
		 * The cap can be exceeded for real: gp_anser_max_info_size is USERSET,
		 * and a gang busy with a named portal does not receive a dispatched
		 * SET -- see cdbdisp_markNamedPortalGangsDestroyed().  So this is a
		 * reachable disagreement, not a corrupt-stream-only case.
		 *
		 * pq_getmessage clears the reading-message flag when it succeeds but
		 * not on its EOF paths; clear it by hand so the error path does not
		 * also trip the assertion in a later pq_startmsgread.
		 */
		pq_endmsgread();
		pfree(buf.data);
		ereport(FATAL,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("anser: could not read the filter message"),
				 errdetail("The message boundary was lost; this connection cannot be used further.")));
	}

	paytype = (char) pq_getmsgint(&buf, 4);
	crc = (uint32) pq_getmsgint(&buf, 4);
	condid = pq_getmsgint(&buf, 4);
	session_id = (uint32) pq_getmsgint(&buf, 4);
	command_count = (uint32) pq_getmsgint(&buf, 4);
	flags = pq_getmsgint(&buf, 4);
	total_len = (uint32) pq_getmsgint(&buf, 4);
	offset = (uint32) pq_getmsgint(&buf, 4);
	keylen = pq_getmsgint(&buf, 4);
	if (keylen < 0 || keylen >= ANSER_CONDITION_KEY_SIZE)
	{
		pfree(buf.data);
		elog(LOG, "anser: filter message has a bad condition key");
		return false;
	}
	keyptr = pq_getmsgbytes(&buf, keylen);
	chunklen = pq_getmsgint(&buf, 4);
	if (chunklen < 0 || (uint32) chunklen > total_len ||
		total_len > (uint32) gp_anser_max_info_size ||
		offset > total_len || (uint32) chunklen > total_len - offset)
	{
		pfree(buf.data);
		elog(LOG, "anser: filter message has a bad chunk frame");
		return false;
	}
	payptr = chunklen > 0 ? pq_getmsgbytes(&buf, chunklen) : NULL;

	if (crc != (uint32) AnserWirePushCrc(paytype, (uint32) condid,
										 session_id, command_count,
										 (uint32) flags, total_len, offset,
										 keyptr, keylen,
										 payptr, chunklen))
	{
		/*
		 * Drop it and keep waiting: the deadline will expire and this consumer
		 * will run unfiltered.  Using the filter anyway is the one thing we must
		 * not do -- a bit corrupted 1 -> 0 silently drops joinable rows.
		 */
		pfree(buf.data);
		elog(LOG, "anser: checksum mismatch on filter for condition %u; discarding",
			 (uint32) condid);
		return true;
	}

	/*
	 * The identity comes from the wire, never from our own globals: a message
	 * parked from an earlier command keeps that command's ids, so a later
	 * statement can never claim it by matching its own.
	 */
	key.gp_session_id = (int) session_id;
	key.gp_command_count = (int) command_count;
	key.condition_id = (uint32) condid;
	memcpy(key.condition_key, keyptr, keylen);
	key.condition_key[keylen] = '\0';

	/*
	 * A chunk for anything but the live subscription belongs to a delivery
	 * whose consumer has already given up.  Drop it without answering: with no
	 * NEXT the delivery ends at its current chunk, and nothing is left in
	 * flight to break the stream.
	 */
	if (!AnserWaiting || !anser_key_equal(&AnserWaitKey, &key))
	{
		ANSER_DEBUG("anser: seg%d dropping chunk cond=%u off=%u len=%d: not subscribed",
					GpIdentity.segindex, (uint32) condid, offset, chunklen);
		pfree(buf.data);
		return true;
	}

	if (flags & ANSER_WIRE_F_CANCELLED)
	{
		/*
		 * A cancelled delivery is a single chunk with total_len = 0.  It
		 * completes the slot at once; any partial assembly is replaced.
		 */
		if (AnserSlot == NULL)
		{
			oldcxt = MemoryContextSwitchTo(TopMemoryContext);
			AnserSlot = palloc0(sizeof(AnserInboxEntry));
			MemoryContextSwitchTo(oldcxt);
		}
		else if (AnserSlot->payload != NULL)
		{
			pfree(AnserSlot->payload);
		}
		AnserSlot->key = key;
		AnserSlot->payload_type = paytype;
		AnserSlot->payload = NULL;
		AnserSlot->cancelled = true;
		AnserSlot->complete = true;
		AnserSlot->total_len = 0;
		AnserSlot->received = 0;
		INSTR_TIME_SET_CURRENT(AnserSlot->last_chunk_at);
		pfree(buf.data);
		return true;
	}

	if (AnserSlot == NULL)
	{
		if (offset != 0)
		{
			/* The window never reopens: out of order means desync. */
			elog(LOG, "anser: chunk for condition %u starts at %u, expected 0; discarding",
				 (uint32) condid, offset);
			pfree(buf.data);
			return true;
		}

		/*
		 * The slot outlives this call and the memory context it was reached
		 * in, so anchor it somewhere stable; AnserSidebandResetInbox drops it.
		 * The assembly buffer is sized by the declared total_len, already
		 * checked against anser.max_info_size above.
		 */
		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		AnserSlot = palloc0(sizeof(AnserInboxEntry));
		AnserSlot->key = key;
		AnserSlot->payload_type = paytype;
		AnserSlot->total_len = total_len;
		AnserSlot->payload = total_len > 0 ? palloc(total_len) : NULL;
		MemoryContextSwitchTo(oldcxt);
	}
	else if (AnserSlot->complete || AnserSlot->received != offset ||
			 AnserSlot->total_len != total_len ||
			 AnserSlot->payload_type != paytype)
	{
		/*
		 * A duplicate, a gap, or a frame that disagrees with the first chunk:
		 * the delivery is desynced, the stream is not.  The total_len and
		 * payload_type checks are what keep the memcpy below inside the
		 * buffer the first chunk sized.
		 */
		elog(LOG, "anser: chunk for condition %u at %u (total %u), have %zu of %zu; discarding",
			 (uint32) condid, offset, total_len,
			 AnserSlot->received, AnserSlot->total_len);
		pfree(buf.data);
		return true;
	}

	/*
	 * Ask for the following chunk before copying this one, so the coordinator
	 * works while we copy.  A NEXT names the offset we will hold after it.
	 */
	if (AnserSlot->received + (uint32) chunklen < total_len)
	{
		char	   *next = AnserWireFormat(&key, ANSER_WIRE_KIND_NEXT,
										 ANSER_PAYLOAD_NONE,
										 AnserSlot->received + (uint32) chunklen,
										 0, 0, NULL, 0);

		if (!anser_sideband_send(next))
			ANSER_DEBUG("anser: seg%d could not ask for the next chunk of cond=%u",
						GpIdentity.segindex, (uint32) condid);
		pfree(next);
	}

	if (chunklen > 0)
		memcpy(AnserSlot->payload + offset, payptr, chunklen);
	AnserSlot->received += (uint32) chunklen;
	if (AnserSlot->received == total_len)
		AnserSlot->complete = true;
	INSTR_TIME_SET_CURRENT(AnserSlot->last_chunk_at);

	/* Log only the first and last chunk; a delivery is hundreds of them. */
	if (offset == 0 || AnserSlot->complete)
		ANSER_DEBUG("anser: seg%d received cond=%u type=%c chunk=[%u,%zu) of %u cancelled=0",
					GpIdentity.segindex, (uint32) condid, paytype,
					offset, AnserSlot->received, total_len);

#ifdef FAULT_INJECTOR
	/*
	 * Let a test pause a consumer mid-delivery, past its deadline: the next
	 * loop iteration finds it expired and withdraws -- the walk-away path.
	 *
	 * Not a 'sleep' fault: that is a bare nanosleep() (pgsleep.c), which any
	 * signal cuts short, and a QE does get signals mid-query, so the pause
	 * ends well inside the deadline and the whole delivery completes.  So the
	 * fault only says "pause", and this waits out the pause on the monotonic
	 * clock, re-arming after every early wakeup.  It stays cancellable, and
	 * it does not touch the socket: the chunk it fires for stays queued, to
	 * be dropped as unsubscribed after the withdrawal.
	 */
	if (SIMPLE_FAULT_INJECTOR("anser_after_chunk") == FaultInjectorTypeSkip)
	{
		double		pause_ms = (double) gp_anser_timeout_ms + 100;
		instr_time	pause_start;
		instr_time	now;

		INSTR_TIME_SET_CURRENT(pause_start);
		for (;;)
		{
			double		left;

			CHECK_FOR_INTERRUPTS();

			INSTR_TIME_SET_CURRENT(now);
			INSTR_TIME_SUBTRACT(now, pause_start);
			left = pause_ms - INSTR_TIME_GET_MILLISEC(now);
			if (left <= 0)
				break;

			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 (long) left + 1,
							 PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);
		}
	}
#endif

	pfree(buf.data);
	return true;
}

/* Claim the delivery if it is complete. */
static bool
anser_inbox_take(const AnserChannelKey *key, char payload_type, void **payload,
				 Size *payload_len, bool *cancelled)
{
	AnserInboxEntry *entry = AnserSlot;

	if (entry == NULL || !entry->complete)
		return false;

	/*
	 * The slot can only hold the live subscription's delivery, so a mismatch
	 * here is a bug, not a late message: assert in assert builds, and end the
	 * wait without the filter rather than log it every 100 ms until the
	 * deadline.
	 */
	if (!anser_key_equal(&entry->key, key))
	{
		Assert(false);
		elog(LOG, "anser: inbox slot for condition %u does not match the waiter %u",
			 entry->key.condition_id, key->condition_id);
		if (cancelled != NULL)
			*cancelled = true;
		AnserSlot = NULL;
		if (entry->payload != NULL)
			pfree(entry->payload);
		pfree(entry);
		return true;
	}

	/*
	 * Right channel, wrong kind of information.  The coordinator stamps
	 * the type from the parts it folded, so this means a producer and a
	 * consumer disagree about what the channel carries -- claim the entry
	 * to stop waiting on it, and treat it as a cancellation so this
	 * consumer runs unfiltered.  An empty delivery carries no type to
	 * check.
	 */
	if (!entry->cancelled && entry->payload != NULL &&
		entry->payload_type != payload_type)
	{
		elog(WARNING, "anser: condition %u delivered payload type '%c', expected '%c'",
			 key->condition_id, entry->payload_type, payload_type);
		entry->cancelled = true;
	}

	if (cancelled != NULL)
		*cancelled = entry->cancelled;
	if (!entry->cancelled && entry->payload != NULL)
	{
		if (payload != NULL)
		{
			*payload = palloc(entry->total_len);
			memcpy(*payload, entry->payload, entry->total_len);
		}
		if (payload_len != NULL)
			*payload_len = entry->total_len;
	}

	AnserSlot = NULL;
	ANSER_DEBUG("anser: seg%d claimed cond=%u (pushed for ssid=%d ccnt=%d, cancelled=%d)",
				GpIdentity.segindex, key->condition_id,
				entry->key.gp_session_id, entry->key.gp_command_count,
				entry->cancelled ? 1 : 0);
	if (entry->payload != NULL)
		pfree(entry->payload);
	pfree(entry);
	return true;
}

/* Drop a delivery nobody claimed, whatever state it is in. */
void
AnserSidebandResetInbox(void)
{
	AnserWaiting = false;
	anser_slot_free();
}

void
AnserSidebandResetAll(void)
{
	AnserSidebandResetInbox();
	AnserDispatchReset();
}

/*
 * Build a QE -> QD payload: fixed-width header, then the key, then the base64
 * body.  See ansersideband.h for the layout and AnserWireParse() for the
 * reader.
 */
char *
AnserWireFormat(const AnserChannelKey *channel_key, char kind,
				char payload_type, uint32 part_index, uint32 total_parts,
				int flags, const void *payload, Size payload_len)
{
	StringInfoData buf;
	char		hdr[ANSER_WIRE_HDR_LEN + 1];
	int			hdrlen;
	int			keylen = (int) strlen(channel_key->condition_key);
	int			bodylen = 0;
	char	   *body = NULL;
	pg_crc32c	crc;

	/*
	 * What the checksum covers has to be exactly what goes on the wire, so the
	 * bytes it will cover are recorded here, as the body is encoded, rather
	 * than re-derived from the arguments afterwards.  Deriving them again would
	 * mean covering a payload that was suppressed -- a cancelled message
	 * carries no body however it was called -- and the reader, seeing no body,
	 * would compute a different checksum and discard a perfectly good message.
	 */
	const void *crc_body = NULL;
	Size		crc_body_len = 0;

	if (!(flags & ANSER_WIRE_F_CANCELLED) && payload != NULL && payload_len > 0)
	{
		int			maxlen = pg_b64_enc_len((int) payload_len);

		if (maxlen > ANSER_WIRE_MAX_BODYLEN)
		{
			/*
			 * No header can describe a body this large.  gp_anser_max_info_size
			 * keeps us far away from this, so it is a belt-and-braces check on
			 * the field width rather than a reachable path.
			 */
			bodylen = 0;
			flags |= ANSER_WIRE_F_CANCELLED;
		}
		else
		{
			body = palloc(maxlen + 1);
			bodylen = pg_b64_encode((const char *) payload, (int) payload_len,
									body, maxlen);
			if (bodylen < 0)
			{
				pfree(body);
				body = NULL;
				bodylen = 0;
				flags |= ANSER_WIRE_F_CANCELLED;
			}
			else
			{
				crc_body = payload;
				crc_body_len = payload_len;
			}
		}
	}

	Assert(keylen <= ANSER_WIRE_MAX_KEYLEN);

	/*
	 * Format the header with a zero CRC, checksum it together with the key and
	 * the raw body, then overwrite the CRC field in place -- it sits at a fixed
	 * offset, so one pass suffices and the checksum still covers every header
	 * field.  Checksumming the body before base64 rather than after means a
	 * mangled encoding is caught too, and whether the body is covered at all is
	 * the payload type's call (anserpayload.h).
	 */
	hdrlen = snprintf(hdr, sizeof(hdr), ANSER_WIRE_HDR_FORMAT,
					  kind, payload_type,
					  (uint32) channel_key->gp_session_id,
					  (uint32) channel_key->gp_command_count,
					  channel_key->condition_id,
					  part_index, total_parts,
					  (uint32) flags, (uint32) keylen, (uint32) bodylen, 0U);
	if (hdrlen != ANSER_WIRE_HDR_LEN)
	{
		/*
		 * Cannot happen -- every field is width-limited -- but complain rather
		 * than throw: this is reached from producer teardown, and the
		 * coordinator's length cross-check will reject the message anyway,
		 * which costs the filter and not the query.
		 */
		elog(WARNING, "anser: formatted a %d byte header, expected %d",
			 hdrlen, ANSER_WIRE_HDR_LEN);
	}

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, hdr, ANSER_WIRE_CRC_OFFSET);
	COMP_CRC32C(crc, channel_key->condition_key, keylen);
	if (crc_body != NULL && AnserPayloadChecksumsBody(payload_type))
		COMP_CRC32C(crc, crc_body, crc_body_len);
	FIN_CRC32C(crc);
	snprintf(hdr + ANSER_WIRE_CRC_OFFSET, 9, "%08x", (uint32) crc);

	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, hdr, ANSER_WIRE_HDR_LEN);
	appendBinaryStringInfo(&buf, channel_key->condition_key, keylen);
	if (bodylen > 0)
		appendBinaryStringInfo(&buf, body, bodylen);

	if (body != NULL)
		pfree(body);

	return buf.data;
}

/*
 * Checksum of a QD -> QE push.  Lives here, on the reading side, but is called
 * from anserdispatch.c too: one definition means the writer and the reader
 * cannot disagree about what is covered -- including whether the body is, which
 * follows from the payload type.  Integers go in network byte order so the
 * value does not depend on the host.
 */
pg_crc32c
AnserWirePushCrc(char payload_type, uint32 condition_id, uint32 session_id,
				 uint32 command_count, uint32 flags, uint32 total_len,
				 uint32 offset, const char *key, int keylen,
				 const void *body, int bodylen)
{
	pg_crc32c	crc;
	uint32		fields[8];
	uint32		belen;

	fields[0] = pg_hton32((uint32) (unsigned char) payload_type);
	fields[1] = pg_hton32(condition_id);
	fields[2] = pg_hton32(session_id);
	fields[3] = pg_hton32(command_count);
	fields[4] = pg_hton32(flags);
	fields[5] = pg_hton32(total_len);
	fields[6] = pg_hton32(offset);
	fields[7] = pg_hton32((uint32) keylen);

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, fields, sizeof(fields));
	if (keylen > 0)
		COMP_CRC32C(crc, key, keylen);
	belen = pg_hton32((uint32) bodylen);
	COMP_CRC32C(crc, &belen, sizeof(belen));
	if (bodylen > 0 && AnserPayloadChecksumsBody(payload_type))
		COMP_CRC32C(crc, body, bodylen);
	FIN_CRC32C(crc);

	return crc;
}

/*
 * Hand a payload to the coordinator.
 *
 * NotifyMyFrontEnd enforces no length limit of its own -- the ~8 KB cap
 * applies to the SQL-level NOTIFY, which has to fit its queue page -- so a
 * multi-megabyte part is fine here.  The payload is base64, hence free of the
 * NUL that would truncate it in pq_sendstring().
 */
static bool
anser_sideband_send(const char *payload)
{
	if (whereToSendOutput != DestRemote)
	{
		/*
		 * Reached by any process that runs the plan without a frontend to
		 * answer to.  Both directions of the protocol come through here, so
		 * this is the single place that says "this process is not on the
		 * sideband at all" -- for a publisher as much as for a subscriber.
		 */
		ANSER_DEBUG("anser: seg%d has no frontend to send on (role=%d writer=%d dest=%d port=%s sock=%d)",
					GpIdentity.segindex, (int) Gp_role, Gp_is_writer ? 1 : 0,
					(int) whereToSendOutput,
					MyProcPort != NULL ? "ok" : "NULL",
					MyProcPort != NULL ? (int) MyProcPort->sock : -1);
		return false;
	}

	NotifyMyFrontEnd(ANSER_NOTIFY_CHANNEL, payload, gp_session_id);
	pq_flush();
	return true;
}
