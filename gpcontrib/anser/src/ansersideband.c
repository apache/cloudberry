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
#include "storage/latch.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/* How long to sleep between wakeups while waiting for a delivery. */
#define ANSER_SIDEBAND_POLL_MS	100L

/*
 * A delivery that arrived while we were waiting for a different channel.
 *
 * One slice can host consumers for more than one condition, and the
 * coordinator pushes each channel as soon as it completes, so messages can
 * arrive in an order we did not ask for.  Rather than discard them (which
 * would cost that consumer its filter), park them here and check the inbox
 * before touching the socket.
 */
typedef struct AnserInboxEntry
{
	AnserChannelKey key;
	char		payload_type;
	char	   *payload;		/* NULL when cancelled */
	Size		payload_len;
	bool		cancelled;
} AnserInboxEntry;

static List *AnserInbox = NIL;

static bool anser_sideband_send(const char *payload);
static bool anser_inbox_take(const AnserChannelKey *key, char payload_type,
							 void **payload, Size *payload_len, bool *cancelled);
static bool anser_sideband_read_one(long timeout_ms);

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
	TimestampTz start;
	int			reads = 0;

	if (payload != NULL)
		*payload = NULL;
	if (payload_len != NULL)
		*payload_len = 0;
	if (cancelled != NULL)
		*cancelled = false;

	if (channel_key == NULL || MyProcPort == NULL ||
		MyProcPort->sock == PGINVALID_SOCKET)
		return false;

	/* It may already be here: the coordinator pushes as soon as it can. */
	if (anser_inbox_take(channel_key, payload_type, payload, payload_len, cancelled))
		return payload != NULL && *payload != NULL;

	/*
	 * A subscription carries nothing, so its payload type is NONE -- what this
	 * consumer expects to receive is checked on delivery, not announced here.
	 */
	msg = AnserWireFormat(channel_key, ANSER_WIRE_KIND_SUBSCRIBE,
						  ANSER_PAYLOAD_NONE, 0, 0, 0, NULL, 0);
	if (!anser_sideband_send(msg))
	{
		pfree(msg);
		return false;
	}
	pfree(msg);
	ANSER_DEBUG("anser: seg%d subscribed cond=%u, waiting up to %ld ms",
				GpIdentity.segindex, channel_key->condition_id, timeout_ms);

	start = GetCurrentTimestamp();
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		if (anser_inbox_take(channel_key, payload_type, payload, payload_len,
							 cancelled))
			return payload != NULL && *payload != NULL;

		if (timeout_ms >= 0 &&
			TimestampDifferenceExceeds(start, GetCurrentTimestamp(), timeout_ms))
		{
			/*
			 * Report what we saw, not just that we waited: "nothing arrived"
			 * and "something arrived for another channel" are different bugs.
			 */
			ANSER_DEBUG("anser: seg%d gave up on cond=%u after %ld ms (read %d message(s), %d unclaimed)",
						GpIdentity.segindex, channel_key->condition_id,
						timeout_ms, reads, list_length(AnserInbox));
			return false;
		}

		/*
		 * Any message we read lands in the inbox; the loop then rechecks
		 * whether it was the one we wanted.  A read failure means the
		 * connection is gone, which the interconnect will report far more
		 * usefully than we can -- stop waiting and let the query run
		 * unfiltered.
		 */
		if (!anser_sideband_read_one(ANSER_SIDEBAND_POLL_MS))
			return false;
		reads = list_length(AnserInbox);
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
	StringInfoData buf;
	AnserInboxEntry *entry;
	MemoryContext oldcxt;
	char		paytype;
	uint32		crc;
	int			condid;
	int			flags;
	int			keylen;
	int			paylen;
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
		elog(LOG, "anser: dispatch connection closed while awaiting a filter");
		return false;
	}

	if (qtype != GP_SIDEBAND_MESSAGE)
	{
		/*
		 * Nothing else should reach us here.  Do not try to interpret or skip
		 * it: message-boundary sync is at stake, so leave it for the command
		 * loop and give up on the filter.
		 */
		pq_endmsgread();
		elog(LOG, "anser: unexpected message type '%c' while awaiting a filter",
			 (char) qtype);
		return false;
	}

	initStringInfo(&buf);
	if (pq_getmessage(&buf, gp_anser_max_info_size + ANSER_CONDITION_KEY_SIZE + 64) != 0)
	{
		/*
		 * pq_getmessage clears the reading-message flag when it succeeds, but
		 * not on its EOF paths; clear it by hand so we do not trip the
		 * assertion in a later pq_startmsgread.
		 */
		pq_endmsgread();
		pfree(buf.data);
		elog(LOG, "anser: could not read filter message");
		return false;
	}

	paytype = (char) pq_getmsgint(&buf, 4);
	crc = (uint32) pq_getmsgint(&buf, 4);
	condid = pq_getmsgint(&buf, 4);
	flags = pq_getmsgint(&buf, 4);
	keylen = pq_getmsgint(&buf, 4);
	if (keylen < 0 || keylen >= ANSER_CONDITION_KEY_SIZE)
	{
		pfree(buf.data);
		elog(LOG, "anser: filter message has a bad condition key");
		return false;
	}
	keyptr = pq_getmsgbytes(&buf, keylen);
	paylen = pq_getmsgint(&buf, 4);
	if (paylen < 0 || paylen > gp_anser_max_info_size)
	{
		pfree(buf.data);
		elog(LOG, "anser: filter message has a bad length");
		return false;
	}
	payptr = paylen > 0 ? pq_getmsgbytes(&buf, paylen) : NULL;

	if (crc != (uint32) AnserWirePushCrc(paytype, (uint32) condid,
											 (uint32) flags, keyptr, keylen,
											 payptr, paylen))
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
	 * The inbox outlives this call and the memory context it was reached in,
	 * so anchor it somewhere stable; AnserSidebandResetAll drops it.
	 */
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	entry = palloc0(sizeof(AnserInboxEntry));
	entry->key.gp_session_id = gp_session_id;
	entry->key.gp_command_count = gp_command_count;
	entry->key.condition_id = (uint32) condid;
	memcpy(entry->key.condition_key, keyptr, keylen);
	entry->key.condition_key[keylen] = '\0';
	entry->payload_type = paytype;
	entry->cancelled = (flags & ANSER_WIRE_F_CANCELLED) != 0;
	if (!entry->cancelled && paylen > 0)
	{
		entry->payload = palloc(paylen);
		memcpy(entry->payload, payptr, paylen);
		entry->payload_len = paylen;
	}
	AnserInbox = lappend(AnserInbox, entry);
	MemoryContextSwitchTo(oldcxt);
	ANSER_DEBUG("anser: seg%d received cond=%u type=%c bytes=%zu cancelled=%d",
				GpIdentity.segindex, (uint32) condid, paytype,
				entry->payload_len, entry->cancelled ? 1 : 0);

	pfree(buf.data);
	return true;
}

/* Claim a delivery for this channel, if one has arrived. */
static bool
anser_inbox_take(const AnserChannelKey *key, char payload_type, void **payload,
				 Size *payload_len, bool *cancelled)
{
	ListCell   *lc;

	foreach(lc, AnserInbox)
	{
		AnserInboxEntry *entry = (AnserInboxEntry *) lfirst(lc);

		if (entry->key.condition_id != key->condition_id ||
			strncmp(entry->key.condition_key, key->condition_key,
					ANSER_CONDITION_KEY_SIZE) != 0)
			continue;

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
				*payload = palloc(entry->payload_len);
				memcpy(*payload, entry->payload, entry->payload_len);
			}
			if (payload_len != NULL)
				*payload_len = entry->payload_len;
		}

		AnserInbox = foreach_delete_current(AnserInbox, lc);
		if (entry->payload != NULL)
			pfree(entry->payload);
		pfree(entry);
		return true;
	}

	return false;
}

/* Drop any deliveries nobody claimed. */
void
AnserSidebandResetInbox(void)
{
	ListCell   *lc;

	foreach(lc, AnserInbox)
	{
		AnserInboxEntry *entry = (AnserInboxEntry *) lfirst(lc);

		if (entry->payload != NULL)
			pfree(entry->payload);
		pfree(entry);
	}
	list_free(AnserInbox);
	AnserInbox = NIL;
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
AnserWirePushCrc(char payload_type, uint32 condition_id, uint32 flags,
				 const char *key, int keylen, const void *body, int bodylen)
{
	pg_crc32c	crc;
	uint32		fields[4];
	uint32		belen;

	fields[0] = pg_hton32((uint32) (unsigned char) payload_type);
	fields[1] = pg_hton32(condition_id);
	fields[2] = pg_hton32(flags);
	fields[3] = pg_hton32((uint32) keylen);

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
		return false;

	NotifyMyFrontEnd(ANSER_NOTIFY_CHANNEL, payload, gp_session_id);
	pq_flush();
	return true;
}
