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

#include <sys/socket.h>

#include "libpq-fe.h"
#include "libpq-events.h"
#include "libpq-int.h"

#include "anser.h"
#include "anserpayload.h"
#include "ansersideband.h"
#include "cdb/cdbconn.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "storage/latch.h"
#include "utils/faultinjector.h"
#include "common/base64.h"
#include "lib/stringinfo.h"
#include "lib/ilist.h"
#include "nodes/pg_list.h"
#include "portability/instr_time.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

/*
 * One in-flight chunked delivery to one consumer.  The merged payload is sent
 * one chunk per GP_SIDEBAND_MESSAGE, and the next chunk goes out only when the
 * consumer asks for it with a NEXT -- so no more than one chunk, always
 * smaller than the connection's send buffer, is ever in flight, and a consumer
 * that walks away between chunks leaves nothing torn behind.
 */
typedef struct AnserDispDelivery
{
	dlist_node active_node;
	struct AnserDispChannel *channel;
	PGconn	   *conn;
	int			segindex;		/* for logs */
	int			chunk_size;		/* chosen once; see anser_disp_chunk_size() */
	uint32		next_offset;	/* sent so far; a NEXT must ask for exactly this */
	bool		started;		/* chunk 0 has been sent */
} AnserDispDelivery;

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
	List	   *deliveries;		/* AnserDispDelivery, one per subscribed QE */
} AnserDispChannel;

static HTAB *AnserDispChannels = NULL;

/*
 * The chunk must fit an empty send buffer, so one pqFlushNonBlocking() places
 * it whole and outCount returns to zero.  SO_SNDBUF is halved because Linux
 * reports it doubled (the kernel's bookkeeping share included); the floor
 * keeps a tiny or unreadable buffer from turning the delivery into thousands
 * of round trips.  anser.debug_delivery_chunk_bytes overrides the choice, for
 * tests that want exactly that.
 */
#define ANSER_CHUNK_MAX		(1024 * 1024)
#define ANSER_CHUNK_MIN		(8 * 1024)

/*
 * Connections holding a partially written chunk.  With chunked delivery this
 * is only the insurance case: a chunk is sized to fit an empty send buffer,
 * and anser_disp_send_chunk() gives the socket a bounded moment to drain
 * before recording the connection here.  Reaching this list therefore means
 * the peer has not read for a full second -- it is gone or wedged, and
 * AnserDispatchFlushPending() poisons the connection before the
 * gang is reused.  List nodes live in TopMemoryContext: the list must survive
 * until ExecutorEnd regardless of the context a delivery ran in.
 *
 * Connections are registered with libpq before Anser retains any pointer.
 * PGEVT_CONNDESTROY removes every reference before PQfinish frees a PGconn,
 * including when an InitPlan releases its gangs before ExecutorEnd. A reset
 * drops the old stream's deliveries too. No dispatcher/core hook is needed.
 */
static List *AnserDispPendingPush = NIL;

/* Intrusive index: teardown must not allocate or start a dynahash scan. */
static dlist_head AnserDispDeliveries = DLIST_STATIC_INIT(AnserDispDeliveries);

/*
 * Withdrawals honoured since the backend started.  Only a test reads it: an
 * unsubscribe leaves no other trace, because the channel it touches is gone
 * by the time the query returns.  Counting them is how we tell "the consumer
 * withdrew and the coordinator acted on it" from "the filter happened to
 * arrive in time", which look identical from SQL.
 */
static int64 AnserDispUnsubscribes = 0;

/*
 * Deliveries that reached their last chunk.  Only a test reads it: a
 * completed delivery leaves no other trace, and a test that wants to know the
 * filter really arrived -- rather than the consumer quietly giving up and
 * running unfiltered, which produces the same rows -- reads the difference.
 */
static int64 AnserDispDeliveriesCompleted = 0;
static MemoryContext AnserDispContext = NULL;

static AnserDispChannel *anser_disp_lookup(const AnserChannelKey *key, bool create);
static void anser_disp_apply_part(AnserDispChannel *chan,
								  const AnserPayloadOps *ops,
								  const void *payload, Size payload_len,
								  int total_parts, bool cancelled);
static void anser_disp_deliver(AnserDispChannel *chan);
static int	anser_disp_chunk_size(PGconn *conn);
static AnserDispDelivery *anser_disp_delivery_find(AnserDispChannel *chan,
												   PGconn *conn);
static void anser_disp_delivery_drop(AnserDispChannel *chan,
									 AnserDispDelivery *d);
static void anser_disp_send_chunk(AnserDispChannel *chan, AnserDispDelivery *d);

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

/* PQsocket() equivalent; same linkage note as above. */
static inline int
anser_conn_socket(const PGconn *conn)
{
	return conn->sock;
}

/*
 * How many withdrawals this backend has honoured.  For tests; see the counter.
 */
int64
AnserDispatchUnsubscribeCount(void)
{
	return AnserDispUnsubscribes;
}

/*
 * How many deliveries this backend has completed.  For tests; see the counter.
 */
int64
AnserDispatchDeliveriesCompleted(void)
{
	return AnserDispDeliveriesCompleted;
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
	/*
	 * Drop the pending-delivery list too, but only the list nodes: the
	 * connections it names may already be dead on the abort path, and
	 * dereferencing them is reserved to cleanup while their gang is live (see the
	 * invariant at the declaration).  A delivery dropped here unflushed is
	 * the cleanupQE guard's problem, which poisons the connection at recycle.
	 */
	list_free(AnserDispPendingPush);
	AnserDispPendingPush = NIL;

	if (AnserDispChannels == NULL)
		return;

	ANSER_DEBUG("anser: QD reset drops %ld channel(s)",
				hash_get_num_entries(AnserDispChannels));

	hash_destroy(AnserDispChannels);
	AnserDispChannels = NULL;
	MemoryContextDelete(AnserDispContext);
	AnserDispContext = NULL;
	dlist_init(&AnserDispDeliveries);
}

/*
 * Drop the finished channels of one command, leaving every other query's
 * alone.
 *
 * AnserDispatchReset() is wholesale, so it can only run once nothing else is
 * executing.  Inside SPI that is not until the outermost statement ends, which
 * holds every inner query's merged filter -- at least anser.max_info_size each
 * -- for the whole call.  The key carries the command count, so a query can
 * take its own channels with it instead of waiting for that sweep.
 *
 * Only channels that are complete and have no delivery in flight are taken,
 * because the caller cannot always prove the count is its own: gp_command_count
 * names the newest query descriptor, not the one that is ending, and the two
 * differ whenever a portal outlives the statements that ran after it.  Closing
 * such a cursor from inside a live query would otherwise hand us that live
 * query's count.  A channel that is complete and already delivered costs at
 * most a consumer that had not subscribed yet, which fails open; an exchange
 * still in flight is never touched, and a channel with a delivery in flight
 * keeps its payload until the sweep.
 *
 * The flip side is that a channel that never completed -- a squelched producer,
 * say -- still waits for the sweep.
 */
void
AnserDispatchResetCommand(int command_count)
{
	HASH_SEQ_STATUS status;
	AnserDispChannel *chan;

	if (AnserDispChannels == NULL)
		return;

	hash_seq_init(&status, AnserDispChannels);
	while ((chan = (AnserDispChannel *) hash_seq_search(&status)) != NULL)
	{
		if (chan->key.gp_command_count != command_count)
			continue;
		if (!chan->complete || chan->deliveries != NIL)
			continue;

		ANSER_DEBUG("anser: QD dropping finished channel cond=%u ccnt=%d",
					chan->key.condition_id, chan->key.gp_command_count);

		/*
		 * AnserDispContext outlives this, so free what the entry owns rather
		 * than the context.  Deleting the entry the scan just handed back is
		 * explicitly allowed; see hash_seq_search().
		 */
		if (chan->payload != NULL)
			pfree(chan->payload);

		if (hash_search(AnserDispChannels, &chan->key,
						HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "anser: dispatch channel hash table corrupted");
	}
}

/*
 * Poison torn deliveries before the connections can be reused. Teardown may
 * run while handling another error, so it must not wait, allocate or report
 * another error. timeout_ms remains in the public API for compatibility.
 */
void
AnserDispatchFlushPending(long timeout_ms)
{
	ListCell *lc;

	(void) timeout_ms;

	foreach(lc, AnserDispPendingPush)
	{
		PGconn *conn = (PGconn *) lfirst(lc);

		if (conn->outCount > 0)
			conn->status = CONNECTION_BAD;
	}
	list_free(AnserDispPendingPush);
	AnserDispPendingPush = NIL;
}

/* list_delete_ptr can allocate in DEBUG_LIST_MEMORY_USAGE builds. Teardown
 * needs the ordinary in-place deletion even in those builds. */
static List *
anser_disp_list_forget(List *list, void *value)
{
	int i;

	for (i = 0; i < list_length(list); ++i)
	{
		if (lfirst(&list->elements[i]) != value)
			continue;
		if (list->length == 1)
		{
			list_free(list);
			return NIL;
		}
		memmove(&list->elements[i], &list->elements[i + 1],
				(list->length - i - 1) * sizeof(ListCell));
		--list->length;
		break;
	}
	return list;
}

/* libpq invokes this while conn is still alive, including on error cleanup.
 * Never allocate, log, wait, or traverse dynahash in the destruction path. */
static void
anser_disp_forget_conn(PGconn *conn)
{
	dlist_mutable_iter iter;

	AnserDispPendingPush = anser_disp_list_forget(AnserDispPendingPush, conn);
	dlist_foreach_modify(iter, &AnserDispDeliveries)
	{
		AnserDispDelivery *d = dlist_container(AnserDispDelivery, active_node, iter.cur);

		if (d->conn == conn)
			anser_disp_delivery_drop(d->channel, d);
	}
}

static int
anser_disp_conn_event(PGEventId event, void *info, void *pass_through)
{
	switch (event)
	{
		case PGEVT_CONNDESTROY:
			anser_disp_forget_conn(((PGEventConnDestroy *) info)->conn);
			break;
		case PGEVT_CONNRESET:
			anser_disp_forget_conn(((PGEventConnReset *) info)->conn);
			break;
		default:
			break;
	}
	return 1;
}

/*
 * Register using the same PGEvent layout and libc allocation as
 * PQregisterEventProc. Cloudberry hides public PQ* symbols in postgres;
 * linking a second libpq to operate on the backend's connections is unsafe.
 * Anser already requires the matching libpq-int.h ABI for its message writer.
 * Registration happens during normal notify processing, never cleanup. On
 * allocation failure refuse the subscription, which then times out unfiltered.
 */
static bool
anser_disp_track_conn(PGconn *conn)
{
	int i;
	char *name;
	PGEvent *event;

	if (conn == NULL)
		return false;
	for (i = 0; i < conn->nEvents; ++i)
		if (conn->events[i].proc == anser_disp_conn_event)
			return true;

	name = strdup("Anser dispatch delivery");
	if (name == NULL)
		return false;
	if (conn->nEvents >= conn->eventArraySize)
	{
		int capacity;
		PGEvent *events;

		if (conn->eventArraySize > INT_MAX / 2)
		{
			free(name);
			return false;
		}
		capacity = conn->eventArraySize ? conn->eventArraySize * 2 : 8;
		events = realloc(conn->events, (size_t) capacity * sizeof(PGEvent));
		if (events == NULL)
		{
			free(name);
			return false;
		}
		conn->events = events;
		conn->eventArraySize = capacity;
	}
	event = &conn->events[conn->nEvents++];
	event->proc = anser_disp_conn_event;
	event->name = name;
	event->passThrough = NULL;
	event->data = NULL;
	event->resultInitialized = false;
	return true;
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
		chan->deliveries = NIL;
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

	/*
	 * NEXT names a channel that must already exist: one that does not was
	 * dropped with its query, and the request is simply late.
	 */
	if (msg.kind == ANSER_WIRE_KIND_NEXT)
	{
		PGconn	   *conn = dr->segdbDesc->conn;
		AnserDispDelivery *d;

		chan = anser_disp_lookup(&msg.key, false);
		d = chan != NULL ? anser_disp_delivery_find(chan, conn) : NULL;

		/*
		 * A NEXT for a delivery we do not have, or for an offset we already
		 * passed, is a late duplicate: the delivery it belongs to is over or
		 * never started.  Skipping it costs nothing -- the consumer's deadline
		 * handles the rest.
		 */
		if (d == NULL || (uint32) msg.part_index != d->next_offset)
		{
			ANSER_DEBUG("anser: QD ignoring NEXT cond=%u from seg%d (offset %d, channel %s)",
						msg.key.condition_id, sender, msg.part_index,
						chan == NULL ? "gone" : "mismatched");
		}
		else
			anser_disp_send_chunk(chan, d);

		MemoryContextSwitchTo(oldcxt);
		return true;
	}

	chan = anser_disp_lookup(&msg.key, true);
	if (chan == NULL)
	{
		MemoryContextSwitchTo(oldcxt);
		return true;
	}

	if (msg.kind == ANSER_WIRE_KIND_SUBSCRIBE)
	{
		PGconn	   *conn = dr->segdbDesc->conn;
		AnserDispDelivery *d;

		if (!anser_disp_track_conn(conn))
		{
			MemoryContextSwitchTo(oldcxt);
			return true;
		}

		/*
		 * A second subscribe from the same connection restarts its delivery
		 * from the beginning -- it cannot mean anything else, because a
		 * consumer subscribes once per wait.
		 */
		d = anser_disp_delivery_find(chan, conn);
		if (d != NULL)
			anser_disp_delivery_drop(chan, d);

		d = (AnserDispDelivery *) palloc0(sizeof(AnserDispDelivery));
		d->conn = conn;
		d->segindex = sender;
		d->chunk_size = anser_disp_chunk_size(conn);
		d->next_offset = 0;
		d->started = false;
		chan->deliveries = lappend(chan->deliveries, d);
		d->channel = chan;
		dlist_push_head(&AnserDispDeliveries, &d->active_node);

		/*
		 * A consumer can subscribe after the channel is already complete --
		 * producers on other segments may well have finished first -- so
		 * start its delivery immediately in that case.
		 */
		ANSER_DEBUG("anser: QD subscribe cond=%u from seg%d (channel %s)",
					msg.key.condition_id, sender,
					chan->complete ? "complete, delivering now" : "still collecting");
		if (chan->complete)
			anser_disp_send_chunk(chan, d);
	}
	else if (msg.kind == ANSER_WIRE_KIND_UNSUBSCRIBE)
	{
		PGconn	   *conn = dr->segdbDesc->conn;
		AnserDispDelivery *d = anser_disp_delivery_find(chan, conn);

		/*
		 * The consumer ran out of time and is no longer reading for us.
		 * Dropping the delivery stops the flow at the chunk boundary: at most
		 * one whole chunk is in flight, and the reader discards it.
		 */
		ANSER_DEBUG("anser: QD unsubscribe cond=%u from seg%d (%d deliver(ies) before)",
					msg.key.condition_id, sender,
					list_length(chan->deliveries));
		if (d != NULL)
			anser_disp_delivery_drop(chan, d);
		AnserDispUnsubscribes++;
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

/* Start every delivery that has not started yet; deliveries stay on the
 * channel until their last chunk. */
static void
anser_disp_deliver(AnserDispChannel *chan)
{
	/*
	 * Hold the delivery back, so a test can let every consumer reach its
	 * deadline first and then be delivered to anyway.  That ordering is what
	 * puts a filter on a connection nobody is reading, and it cannot be
	 * arranged with data: the channel completes when the last part lands,
	 * which is normally while the consumers are still waiting for it.
	 */
	SIMPLE_FAULT_INJECTOR("anser_before_deliver");

	ANSER_DEBUG("anser: QD delivering cond=%u to %d subscriber(s)",
				chan->key.condition_id, list_length(chan->deliveries));

	/*
	 * send_chunk() can drop the delivery it is given (write failure), so this
	 * cannot be a foreach(): restart from the first unstarted delivery each
	 * time.  Every call either starts or removes one, so it terminates.
	 */
	for (;;)
	{
		ListCell   *lc;
		AnserDispDelivery *d = NULL;

		foreach(lc, chan->deliveries)
		{
			AnserDispDelivery *e = (AnserDispDelivery *) lfirst(lc);

			if (!e->started)
			{
				d = e;
				break;
			}
		}
		if (d == NULL)
			break;
		anser_disp_send_chunk(chan, d);
	}
}

/* The delivery of one connection, or NULL. */
static AnserDispDelivery *
anser_disp_delivery_find(AnserDispChannel *chan, PGconn *conn)
{
	ListCell   *lc;

	foreach(lc, chan->deliveries)
	{
		AnserDispDelivery *d = (AnserDispDelivery *) lfirst(lc);

		if (d->conn == conn)
			return d;
	}
	return NULL;
}

/* Forget one delivery: the consumer is gone or the delivery is done. */
static void
anser_disp_delivery_drop(AnserDispChannel *chan, AnserDispDelivery *d)
{
	chan->deliveries = anser_disp_list_forget(chan->deliveries, d);
	dlist_delete(&d->active_node);
	pfree(d);
}

/*
 * The chunk size for one connection, chosen once when the delivery starts.
 * The debug GUC wins so a test can force hundreds of round trips; otherwise
 * halve the send buffer (see the constants' comment) and clamp into range.
 */
static int
anser_disp_chunk_size(PGconn *conn)
{
	int			sndbuf = 0;
	socklen_t	len = sizeof(sndbuf);
	int			size;

	if (gp_anser_debug_delivery_chunk_bytes > 0)
		return Max(gp_anser_debug_delivery_chunk_bytes, ANSER_CHUNK_MIN);

	/*
	 * conn can be NULL here: the dispatcher NULLs it after PQfinish on the
	 * error/FTS paths and treats that as a dead connection
	 * (cdbconn_isBadConnection), so do not rely on the notify handler only
	 * seeing live ones.
	 */
	if (conn != NULL && conn->sock != PGINVALID_SOCKET &&
		getsockopt(conn->sock, SOL_SOCKET, SO_SNDBUF, &sndbuf, &len) == 0 &&
		sndbuf > 0)
		size = sndbuf / 2;
	else
		size = ANSER_CHUNK_MIN;

	return Min(ANSER_CHUNK_MAX, Max(ANSER_CHUNK_MIN, size));
}

/*
 * Write the next chunk of one delivery as one GP_SIDEBAND_MESSAGE.
 *
 * The chunk is sized to fit an empty send buffer, so the flush below places
 * it whole and nothing is left queued.  The insurance for a wrong estimate is
 * a bounded wait -- one second, no CHECK_FOR_INTERRUPTS and no dispatcher
 * calls, because this runs inside the dispatcher's notify processing -- after
 * which the connection goes on the pending list for AnserDispatchFlushPending()
 * and the delivery is over: the consumer's own deadline ends its wait.
 *
 * Delivery is per consumer: a write that fails costs that one segment its
 * filter and leaves the others alone.
 */
static void
anser_disp_send_chunk(AnserDispChannel *chan, AnserDispDelivery *d)
{
	PGconn	   *conn = d->conn;
	int			flags = chan->cancelled ? ANSER_WIRE_F_CANCELLED : 0;
	uint32		total_len = chan->cancelled ? 0 : (uint32) chan->payload_len;
	uint32		offset = chan->cancelled ? 0 : d->next_offset;
	int			chunk_len = chan->cancelled ? 0 :
		(int) Min((uint32) d->chunk_size, total_len - offset);
	int			keylen = (int) strlen(chan->key.condition_key);
	const char *chunk = chunk_len > 0 ? chan->payload + offset : NULL;
	char		paytype;
	pg_crc32c	crc;
	bool		first = !d->started;
	bool		last;

	if (!anser_conn_ok(conn))
		goto out;

	/*
	 * A channel that was cancelled before any part arrived has no type.  That
	 * is fine: the delivery carries no body, and a consumer only checks the
	 * type of a body it actually received.
	 */
	paytype = chan->ops != NULL ? chan->ops->code : ANSER_PAYLOAD_NONE;

	crc = AnserWirePushCrc(paytype, chan->key.condition_id,
						   chan->key.gp_session_id, chan->key.gp_command_count,
						   (uint32) flags, total_len, offset,
						   chan->key.condition_key, keylen,
						   chunk, chunk_len);

	/*
	 * Raw binary: pqPutnchar performs no encoding conversion, so unlike the
	 * QE -> QD direction this needs no base64.  These calls only grow the
	 * output buffer, so nothing here can block.
	 */
	if (pqPutMsgStart(GP_SIDEBAND_MESSAGE, conn) < 0 ||
		pqPutInt((int) paytype, 4, conn) < 0 ||
		pqPutInt((int) crc, 4, conn) < 0 ||
		pqPutInt((int) chan->key.condition_id, 4, conn) < 0 ||
		pqPutInt((int) chan->key.gp_session_id, 4, conn) < 0 ||
		pqPutInt((int) chan->key.gp_command_count, 4, conn) < 0 ||
		pqPutInt(flags, 4, conn) < 0 ||
		pqPutInt((int) total_len, 4, conn) < 0 ||
		pqPutInt((int) offset, 4, conn) < 0 ||
		pqPutInt(keylen, 4, conn) < 0 ||
		pqPutnchar(chan->key.condition_key, keylen, conn) < 0 ||
		pqPutInt(chunk_len, 4, conn) < 0 ||
		(chunk_len > 0 && pqPutnchar(chunk, chunk_len, conn) < 0))
	{
		elog(LOG, "anser: could not deliver filter for condition %u: %s",
			 chan->key.condition_id, anser_conn_error(conn));
		goto out;
	}

	pqPutMsgEndNoAutoFlush(conn);
	if (pqFlushNonBlocking(conn) < 0)
	{
		elog(LOG, "anser: could not deliver filter for condition %u: %s",
			 chan->key.condition_id, anser_conn_error(conn));
		goto out;
	}

	if (conn->outCount > 0)
	{
		/*
		 * The chunk was meant to fit the buffer whole.  It did not, so give
		 * the peer a bounded moment to drain: it just asked for this chunk,
		 * so it is alive and reading.  Over the deadline the connection is
		 * torn and goes on the pending list.
		 */
		instr_time	start;
		instr_time	now;

		INSTR_TIME_SET_CURRENT(start);

		while (conn->outCount > 0)
		{
			if (pqFlushNonBlocking(conn) < 0)
				break;
			if (conn->outCount == 0)
				break;
			INSTR_TIME_SET_CURRENT(now);
			INSTR_TIME_SUBTRACT(now, start);
			if (INSTR_TIME_GET_MILLISEC(now) >= 1000)
				break;

			(void) WaitLatchOrSocket(MyLatch,
									 WL_LATCH_SET | WL_SOCKET_WRITEABLE |
									 WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
									 anser_conn_socket(conn), 100L /* ms */,
									 PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);
		}

		if (conn->outCount > 0 &&
			!list_member_ptr(AnserDispPendingPush, conn))
		{
			MemoryContext savecxt = MemoryContextSwitchTo(TopMemoryContext);

			AnserDispPendingPush = lappend(AnserDispPendingPush, conn);
			MemoryContextSwitchTo(savecxt);
		}
		if (conn->outCount > 0)
		{
			elog(LOG, "anser: cond=%u chunk did not flush in 1 s, %d bytes queued; ending the delivery",
				 chan->key.condition_id, conn->outCount);
			goto out;
		}
	}

	d->started = true;
	d->next_offset = offset + (uint32) chunk_len;

	/* Log only the first and last chunk; a delivery is hundreds of them. */
	last = chan->cancelled || d->next_offset >= total_len;
	if (first || last)
		ANSER_DEBUG("anser: QD pushed cond=%u type=%c to seg%d chunk=[%u,%u) of %u cancelled=%d chunk_size=%d",
					chan->key.condition_id, paytype, d->segindex,
					offset, d->next_offset, total_len,
					chan->cancelled ? 1 : 0, d->chunk_size);
	if (last)
	{
		/*
		 * Counted only when a filter really travelled: a cancelled delivery
		 * tells the consumer to run unfiltered, so it must not look like an
		 * arrival to a test.
		 */
		if (!chan->cancelled)
			AnserDispDeliveriesCompleted++;
		goto out;
	}
	return;

out:
	anser_disp_delivery_drop(chan, d);
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

	if (kind != ANSER_WIRE_KIND_PART &&
		kind != ANSER_WIRE_KIND_SUBSCRIBE &&
		kind != ANSER_WIRE_KIND_UNSUBSCRIBE &&
		kind != ANSER_WIRE_KIND_NEXT)
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
