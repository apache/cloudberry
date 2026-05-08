/*-------------------------------------------------------------------------
 *
 * ic_common.c
 *	   Interconnect code shared between UDP, and TCP IPC Layers.
 *
 * IDENTIFICATION
 *	    contrib/interconnect/ic_common.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "ic_common.h"
#include "ic_modules.h"
#include "common/ip.h"
#include "nodes/execnodes.h"	/* ExecSlice, SliceTable */
#include "miscadmin.h"
#include "libpq/libpq-be.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "cdb/ml_ipc.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp.h"

#include <unistd.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <netinet/in.h>

static interconnect_handle_t * open_interconnect_handles;
static bool interconnect_resowner_callback_registered;

/*=========================================================================
 * VISIBLE FUNCTIONS
 */

/* See ml_ipc.h */
bool
SendTupleChunkToAMS(ChunkTransportState * transportStates,
					int16 motNodeID,
					int16 targetRoute,
					TupleChunkListItem tcItem)
{
	int			i,
				recount = 0;
	ChunkTransportStateEntry *pEntry = NULL;
	MotionConn *conn = NULL;
	TupleChunkListItem currItem;

	if (!transportStates)
		elog(FATAL, "SendTupleChunkToAMS: no transport-states.");
	if (!transportStates->activated)
		elog(FATAL, "SendTupleChunkToAMS: transport states inactive");

	/* check em' */
	ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);

#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG3, "sendtuplechunktoams: calling get_transport_state"
		 "w/transportStates %p transportState->size %d motnodeid %d route %d",
		 transportStates, transportStates->size, motNodeID, targetRoute);
#endif

	getChunkTransportState(transportStates, motNodeID, &pEntry);

	/*
	 * tcItem can actually be a chain of tcItems.  we need to send out all of
	 * them.
	 */
	for (currItem = tcItem; currItem != NULL; currItem = currItem->p_next)
	{
#ifdef AMS_VERBOSE_LOGGING
		elog(DEBUG5, "SendTupleChunkToAMS: chunk length %d", currItem->chunk_length);
#endif

		if (targetRoute == BROADCAST_SEGIDX)
		{
			doBroadcast(transportStates, pEntry, currItem, &recount);
		}
		else
		{
			if (targetRoute < 0 || targetRoute >= pEntry->numConns)
			{
				elog(FATAL, "SendTupleChunkToAMS: targetRoute is %d, must be between 0 and %d .",
					 targetRoute, pEntry->numConns);
			}
			/* handle pt-to-pt message. Primary */
			getMotionConn(pEntry, targetRoute, &conn);

			/* only send to interested connections */
			if (conn->stillActive)
			{
				CurrentMotionIPCLayer->SendChunk(transportStates, pEntry, conn, currItem, motNodeID);
				if (!conn->stillActive)
					recount = 1;
			}
			/* in 4.0 logical mirror xmit eliminated. */
		}
	}

	if (recount == 0)
		return true;

	/* if we don't have any connections active, return false */
	for (i = 0; i < pEntry->numConns; i++)
	{
		getMotionConn(pEntry, i, &conn);
		if (conn->stillActive)
			break;
	}

	/* if we found an active connection we're not done */
	return (i < pEntry->numConns);
}

/*
 * The fetches a direct pointer into our transmit buffers, along with
 * an indication as to how much data can be safely shoved into the
 * buffer (started at the pointed location).
 *
 * This works a lot like SendTupleChunkToAMS().
 */
void
GetTransportDirectBuffer(ChunkTransportState * transportStates,
						 int16 motNodeID,
						 int16 targetRoute,
						 struct directTransportBuffer *b)
{
	ChunkTransportStateEntry *pEntry = NULL;
	MotionConn *conn = NULL;

	if (!transportStates)
	{
		elog(FATAL, "GetTransportDirectBuffer: no transport states");
	}
	else if (!transportStates->activated)
	{
		elog(FATAL, "GetTransportDirectBuffer: inactive transport states");
	}
	else if (targetRoute == BROADCAST_SEGIDX)
	{
		elog(FATAL, "GetTransportDirectBuffer: can't direct-transport to broadcast");
	}

	Assert(b != NULL);

	do
	{
		getChunkTransportState(transportStates, motNodeID, &pEntry);

		/* handle pt-to-pt message. Primary */
		getMotionConn(pEntry, targetRoute, &conn);

		/* only send to interested connections */
		if (!conn->stillActive)
		{
			break;
		}

		b->pri = conn->pBuff + conn->msgSize;
		b->prilen = Gp_max_packet_size - conn->msgSize;

		/* got buffer. */
		return;
	}
	while (0);

	/* buffer is missing ? */

	b->pri = NULL;
	b->prilen = 0;

	return;
}

/*
 * The fetches a direct pointer into our transmit buffers, along with
 * an indication as to how much data can be safely shoved into the
 * buffer (started at the pointed location).
 *
 * This works a lot like SendTupleChunkToAMS().
 */
void
PutTransportDirectBuffer(ChunkTransportState * transportStates,
						 int16 motNodeID,
						 int16 targetRoute, int length)
{
	ChunkTransportStateEntry *pEntry = NULL;
	MotionConn *conn = NULL;

	if (!transportStates)
	{
		elog(FATAL, "PutTransportDirectBuffer: no transport states");
	}
	else if (!transportStates->activated)
	{
		elog(FATAL, "PutTransportDirectBuffer: inactive transport states");
	}
	else if (targetRoute == BROADCAST_SEGIDX)
	{
		elog(FATAL, "PutTransportDirectBuffer: can't direct-transport to broadcast");
	}

	getChunkTransportState(transportStates, motNodeID, &pEntry);

	/* handle pt-to-pt message. Primary */
	getMotionConn(pEntry, targetRoute, &conn);
	/* only send to interested connections */
	if (conn->stillActive)
	{
		conn->msgSize += length;
		conn->tupleCount++;
	}

	/* put buffer. */
	return;
}

/*=========================================================================
 * HELPER FUNCTIONS
 */


/* Function createChunkTransportState() is used to create a ChunkTransportState struct and
 * place it in the hashtab hashtable based on the motNodeID.
 *
 * PARAMETERS
 *
 *	 motNodeID - motion node ID for this ChunkTransportState.
 *
 *	 numConns  - number of primary connections for this motion node.
 *               All are incoming if this is a receiving motion node.
 *               All are outgoing if this is a sending motion node.
 *
 * RETURNS
 *	 An empty and initialized ChunkTransportState struct for the given motion node. If
 *	 a ChuckTransportState struct is already registered for the motNodeID an ERROR is
 *	 thrown.
 */
ChunkTransportStateEntry *
createChunkTransportState(ChunkTransportState * transportStates,
						  ExecSlice * sendSlice,
						  ExecSlice * recvSlice,
						  int numConns,
						  size_t chunk_trans_state_entry_size)
{
	ChunkTransportStateEntry *pEntry = NULL;
	int			motNodeID;
	int			i;

	Assert(recvSlice->sliceIndex >= 0);
	Assert(sendSlice->sliceIndex > 0);

	motNodeID = sendSlice->sliceIndex;
	if (motNodeID > transportStates->size)
	{
		/* increase size of our table */
		ChunkTransportStateEntry *newTable;
		size_t		old_states_pos = transportStates->size * chunk_trans_state_entry_size;

		newTable = repalloc(transportStates->states, motNodeID * chunk_trans_state_entry_size);
		transportStates->states = newTable;
		/* zero-out the new piece at the end */
		MemSet(((uint8 *) transportStates->states) + old_states_pos, 0, (motNodeID - transportStates->size) * chunk_trans_state_entry_size);
		transportStates->size = motNodeID;
	}

	getChunkTransportStateNoValid(transportStates, motNodeID, &pEntry);

	if (pEntry->valid)
	{
		MotionConn *conn = NULL;

		getMotionConn(pEntry, 0, &conn);
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("interconnect error: A HTAB entry for motion node %d already exists",
						motNodeID),
				 errdetail("conns %p numConns %d first sock %d",
						   pEntry->conns, pEntry->numConns,
						   conn != NULL ? conn->sockfd : -2)));
	}

	pEntry->valid = true;

	pEntry->motNodeId = motNodeID;
	pEntry->numConns = numConns;
	pEntry->scanStart = 0;
	pEntry->sendSlice = sendSlice;
	pEntry->recvSlice = recvSlice;

	allocMotionConns(pEntry);

	for (i = 0; i < pEntry->numConns; i++)
	{
		MotionConn *conn = NULL;
		getMotionConn(pEntry, i, &conn);

		/* Initialize MotionConn entry. */
		conn->state = mcsNull;
		conn->sockfd = -1;
		conn->msgSize = 0;
		conn->tupleCount = 0;
		conn->stillActive = false;
		conn->stopRequested = false;
		conn->cdbProc = NULL;
		conn->remapper = NULL;
		conn->sent_record_typmod = 0;
	}

	return pEntry;
}

/* Function removeChunkTransportState() is used to remove a ChunkTransportState struct from
 * the hashtab hashtable.
 *
 * This should only be called after createChunkTransportState().
 *
 * PARAMETERS
 *
 *	 motNodeID - motion node ID to lookup the ChunkTransportState.
 *   pIncIdx - parent slice idx in child slice.  If not multiplexed, should be 1.
 *
 * RETURNS
 *	 The ChunkTransportState that was removed from the hashtab hashtable.
 */
ChunkTransportStateEntry *
removeChunkTransportState(ChunkTransportState * transportStates,
						  int16 motNodeID)
{
	ChunkTransportStateEntry *pEntry = NULL;

	if (motNodeID > transportStates->size)
	{
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("interconnect error: Unexpected Motion Node Id: %d",
						motNodeID),
				 errdetail("During remove. (size %d)", transportStates->size)));
	}


	getChunkTransportStateNoValid(transportStates, motNodeID, &pEntry);

	if (!pEntry->valid)
	{
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("interconnect error: Unexpected Motion Node Id: %d",
						motNodeID),
				 errdetail("During remove. State not valid")));
	}
	else
	{
		pEntry->valid = false;
	}

	MPP_FD_ZERO(&pEntry->readSet);

	return pEntry;
}

/*
 * checkForCancelFromQD
 * 		Check for cancel from QD.
 *
 * Should be called only inside the dispatcher
 */
void
checkForCancelFromQD(ChunkTransportState * pTransportStates)
{
	Assert(Gp_role == GP_ROLE_DISPATCH);
	Assert(pTransportStates);
	Assert(pTransportStates->estate);

	if (cdbdisp_checkForCancel(pTransportStates->estate->dispatcherState))
	{
		ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						errmsg(CDB_MOTION_LOST_CONTACT_STRING)));
		/* not reached */
	}
}

/*
 * format_sockaddr
 *			Format a sockaddr to a human readable string
 *
 * This function must be kept threadsafe, elog/ereport/palloc etc are not
 * allowed within this function.
 */
char *
format_sockaddr(struct sockaddr_storage *sa, char *buf, size_t len)
{
	int			ret;
	char		remote_host[NI_MAXHOST];
	char		remote_port[NI_MAXSERV];

	ret = pg_getnameinfo_all(sa, sizeof(struct sockaddr_storage),
							 remote_host, sizeof(remote_host),
							 remote_port, sizeof(remote_port),
							 NI_NUMERICHOST | NI_NUMERICSERV);

	if (ret != 0)
		snprintf(buf, len, "?host?:?port?");
	else
	{
#ifdef HAVE_IPV6
		if (sa->ss_family == AF_INET6)
			snprintf(buf, len, "[%s]:%s", remote_host, remote_port);
		else
#endif
			snprintf(buf, len, "%s:%s", remote_host, remote_port);
	}

	return buf;
}

void
destroy_interconnect_handle(interconnect_handle_t * h)
{
	h->interconnect_context = NULL;
	/* unlink from linked list first */
	if (h->prev)
		h->prev->next = h->next;
	else
		open_interconnect_handles = h->next;
	if (h->next)
		h->next->prev = h->prev;

	pfree(h);

	if (open_interconnect_handles == NULL)
		MemoryContextReset(InterconnectContext);
}

static void
cleanup_interconnect_handle(interconnect_handle_t * h)
{
	if (h->interconnect_context == NULL)
	{
		destroy_interconnect_handle(h);
		return;
	}
	h->teardown_cb(h->interconnect_context, true);
}

static void
interconnect_abort_callback(ResourceReleasePhase phase,
							bool isCommit,
							bool isTopLevel,
							void *arg)
{
	interconnect_handle_t *curr;
	interconnect_handle_t *next;

	if (phase != RESOURCE_RELEASE_AFTER_LOCKS)
		return;

	next = open_interconnect_handles;
	while (next)
	{
		curr = next;
		next = curr->next;

		if (curr->owner == CurrentResourceOwner)
		{
			if (isCommit)
				elog(WARNING, "interconnect reference leak: %p still referenced", curr);

			cleanup_interconnect_handle(curr);
		}
	}
}


interconnect_handle_t *
allocate_interconnect_handle(TeardownInterconnectCallBack callback)
{
	interconnect_handle_t *h;

	if (InterconnectContext == NULL)
		InterconnectContext = AllocSetContextCreate(TopMemoryContext,
													"Interconnect Context",
													ALLOCSET_DEFAULT_MINSIZE,
													ALLOCSET_DEFAULT_INITSIZE,
													ALLOCSET_DEFAULT_MAXSIZE);

	h = MemoryContextAllocZero(InterconnectContext, sizeof(interconnect_handle_t));

	h->teardown_cb = callback;
	h->owner = CurrentResourceOwner;
	h->next = open_interconnect_handles;
	h->prev = NULL;
	if (open_interconnect_handles)
		open_interconnect_handles->prev = h;
	open_interconnect_handles = h;

	if (!interconnect_resowner_callback_registered)
	{
		RegisterResourceReleaseCallback(interconnect_abort_callback, NULL);
		interconnect_resowner_callback_registered = true;
	}
	return h;
}

interconnect_handle_t *
find_interconnect_handle(ChunkTransportState * icContext)
{
	interconnect_handle_t *head = open_interconnect_handles;

	while (head != NULL)
	{
		if (head->interconnect_context == icContext)
			return head;
		head = head->next;
	}
	return NULL;
}


TupleRemapper *
GetMotionConnTupleRemapper(ChunkTransportState * transportStates,
						   int16 motNodeID,
						   int16 targetRoute)
{
	ChunkTransportStateEntry *pEntry = NULL;
	MotionConn *conn = NULL;

	getChunkTransportState(transportStates, motNodeID, &pEntry);
	getMotionConn(pEntry, targetRoute, &conn);
	Assert(conn);

	return conn->remapper;
}


int32 *
GetMotionSentRecordTypmod(ChunkTransportState * transportStates,
						   int16 motNodeID,
						   int16 targetRoute)
{
	MotionConn *conn = NULL;
	ChunkTransportStateEntry *pEntry = NULL;
	getChunkTransportState(transportStates, motNodeID, &pEntry);

	if (targetRoute == BROADCAST_SEGIDX) {
		targetRoute = 0;
	}

	getMotionConn(pEntry, targetRoute, &conn);

	return &conn->sent_record_typmod;
}

/*
 * do nothing for tcp/proxy implement.
 */
void
DirectPutRxBufferTCP(ChunkTransportState *transportStates, int motNodeID, int route)
{
	return;
}

/*
 * do nothing for tcp/proxy implement.
 */
uint32
GetActiveMotionConnsTCP(void)
{
	return 0;
}
