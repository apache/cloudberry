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
#include "storage/latch.h"
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
	char	   *payload;		/* NULL when cancelled */
	Size		payload_len;
	bool		cancelled;
} AnserInboxEntry;

static List *AnserInbox = NIL;

static bool anser_sideband_send(const char *payload);
static char *anser_sideband_format(const AnserChannelKey *channel_key, char kind,
								   uint32 part_index, uint32 total_parts,
								   int flags, const void *payload,
								   Size payload_len);
static bool anser_inbox_take(const AnserChannelKey *key, void **payload,
							 Size *payload_len, bool *cancelled);
static bool anser_sideband_read_one(long timeout_ms);

/*
 * Publish one part.  Fire-and-forget: the coordinator does not acknowledge,
 * because nothing on this side needs to wait for it.
 */
bool
AnserSidebandPublish(const AnserChannelKey *channel_key,
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

	msg = anser_sideband_format(channel_key, ANSER_WIRE_KIND_PART,
								part_index, total_parts, flags,
								(flags & ANSER_WIRE_F_CANCELLED) ? NULL : payload,
								(flags & ANSER_WIRE_F_CANCELLED) ? 0 : payload_len);
	ok = anser_sideband_send(msg);
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
AnserSidebandConsumeWait(const AnserChannelKey *channel_key,
						 void **payload, Size *payload_len,
						 bool *cancelled, long timeout_ms)
{
	char	   *msg;
	TimestampTz start;

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
	if (anser_inbox_take(channel_key, payload, payload_len, cancelled))
		return payload != NULL && *payload != NULL;

	msg = anser_sideband_format(channel_key, ANSER_WIRE_KIND_SUBSCRIBE,
								0, 0, 0, NULL, 0);
	if (!anser_sideband_send(msg))
	{
		pfree(msg);
		return false;
	}
	pfree(msg);

	start = GetCurrentTimestamp();
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		if (anser_inbox_take(channel_key, payload, payload_len, cancelled))
			return payload != NULL && *payload != NULL;

		if (timeout_ms >= 0 &&
			TimestampDifferenceExceeds(start, GetCurrentTimestamp(), timeout_ms))
			return false;

		/*
		 * Any message we read lands in the inbox; the loop then rechecks
		 * whether it was the one we wanted.  A read failure means the
		 * connection is gone, which the interconnect will report far more
		 * usefully than we can -- stop waiting and let the query run
		 * unfiltered.
		 */
		if (!anser_sideband_read_one(ANSER_SIDEBAND_POLL_MS))
			return false;
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
	entry->cancelled = (flags & ANSER_WIRE_F_CANCELLED) != 0;
	if (!entry->cancelled && paylen > 0)
	{
		entry->payload = palloc(paylen);
		memcpy(entry->payload, payptr, paylen);
		entry->payload_len = paylen;
	}
	AnserInbox = lappend(AnserInbox, entry);
	MemoryContextSwitchTo(oldcxt);

	pfree(buf.data);
	return true;
}

/* Claim a delivery for this channel, if one has arrived. */
static bool
anser_inbox_take(const AnserChannelKey *key, void **payload,
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

/* Build a QE -> QD payload; see anser_disp_parse() for the layout. */
static char *
anser_sideband_format(const AnserChannelKey *channel_key, char kind,
					  uint32 part_index, uint32 total_parts, int flags,
					  const void *payload, Size payload_len)
{
	StringInfoData buf;
	int			keylen = (int) strlen(channel_key->condition_key);
	int			bodylen = 0;
	char	   *body = NULL;

	if (payload != NULL && payload_len > 0)
	{
		int			maxlen = pg_b64_enc_len((int) payload_len);

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
	}

	initStringInfo(&buf);
	appendStringInfo(&buf, ANSER_WIRE_TAG " %c %d %d %u %u %u %d %d %d\n",
					 kind, channel_key->gp_session_id,
					 channel_key->gp_command_count, channel_key->condition_id,
					 part_index, total_parts, flags, keylen, bodylen);
	appendBinaryStringInfo(&buf, channel_key->condition_key, keylen);
	if (bodylen > 0)
		appendBinaryStringInfo(&buf, body, bodylen);

	if (body != NULL)
		pfree(body);

	return buf.data;
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
