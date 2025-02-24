/*-------------------------------------------------------------------------
 *
 * ic_common.h
 *	   Interconnect code shared between UDP, and TCP IPC Layers.
 *
 * IDENTIFICATION
 *	    contrib/interconnect/ic_common.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef IC_COMMON_H
#define IC_COMMON_H

#include "postgres.h"

#include "ic_internal.h"
#include "common/ip.h"
#include "nodes/execnodes.h"	/* ExecSlice, SliceTable */
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp.h"
#include "executor/execdesc.h"
#include "utils/memutils.h"

typedef void (*TeardownInterconnectCallBack)(ChunkTransportState *transportStates, bool hasErrors);

/*=========================================================================
 * STRUCTS
 */
typedef struct interconnect_handle_t
{
	ChunkTransportState	*interconnect_context; /* Interconnect state */

    // callback for interconnect been abort
    TeardownInterconnectCallBack teardown_cb;

	ResourceOwner owner;	/* owner of this handle */
	struct interconnect_handle_t *next;
	struct interconnect_handle_t *prev;
} interconnect_handle_t;

/*=========================================================================
 * GLOBAL STATE VARIABLES
 */

/* Socket file descriptor for the listener. */
extern int			TCP_listenerFd;
extern int			UDP_listenerFd;

/*=========================================================================
 * Resource manager
 */

void destroy_interconnect_handle(interconnect_handle_t *h);

interconnect_handle_t * allocate_interconnect_handle(TeardownInterconnectCallBack callback);

interconnect_handle_t * find_interconnect_handle(ChunkTransportState *icContext);

/*=========================================================================
 * Common method in IPC layer
 */

extern char *
format_sockaddr(struct sockaddr_storage *sa, char *buf, size_t len);

extern bool SendTupleChunkToAMS(ChunkTransportState *transportStates,
					int16 motNodeID,
					int16 targetRoute,
					TupleChunkListItem tcItem);

extern ChunkTransportStateEntry *createChunkTransportState(ChunkTransportState *transportStates,
														   ExecSlice *sendSlice,
														   ExecSlice *recvSlice,
														   int numConns,
														   size_t chunk_trans_state_entry_size);

extern ChunkTransportStateEntry *removeChunkTransportState(ChunkTransportState *transportStates,
														   int16 motNodeID);


#define ML_CHECK_FOR_INTERRUPTS(teardownActive) \
		do {if (!teardownActive && InterruptPending) CHECK_FOR_INTERRUPTS(); } while (0)

/* doBroadcast() is used to send a TupleChunk to all recipients.
 *
 * PARAMETERS
 *   mlStates - motion-layer state ptr.
 *   transportStates - IC-instance ptr. 
 *	 pChunkEntry - ChunkTransportState context that contains everything we need to send.
 *	 tcItem - TupleChunk to send.
 */
#define doBroadcast(transportStates, pChunkEntry, tcItem, inactiveCountPtr) \
	do { \
		MotionConn *conn = NULL; \
		int			*p_inactive = inactiveCountPtr; \
		int			i, index, inactive = 0; \
		/* add our tcItem to each of the outgoing buffers. */ \
		index = Max(0, GpIdentity.segindex); /* entry-db has -1 */ \
		for (i = 0; i < pChunkEntry->numConns; i++, index++) \
		{ \
			if (index >= pChunkEntry->numConns) \
				index = 0; \
			getMotionConn(pChunkEntry, index, &conn);\
			/* only send to still interested receivers. */ \
			if (conn->stillActive) \
			{ \
				CurrentMotionIPCLayer->SendChunk(transportStates, pChunkEntry, conn, tcItem, pChunkEntry->motNodeId); \
				if (!conn->stillActive) \
					inactive++; \
			} \
		} \
		if (p_inactive != NULL)					\
			*p_inactive = (inactive ? 1 : 0);	\
	} while (0)

/*
 * checkForCancelFromQD
 * 		Check for cancel from QD.
 *
 * Should be called only inside the dispatcher
 */
extern void
checkForCancelFromQD(ChunkTransportState *pTransportStates);

extern void
GetTransportDirectBuffer(ChunkTransportState *transportStates,
						 int16 motNodeID,
						 int16 targetRoute,
						 struct directTransportBuffer *b);

extern void PutTransportDirectBuffer(ChunkTransportState *transportStates,
						 int16 motNodeID,
						 int16 targetRoute, int length);

extern TupleRemapper * GetMotionConnTupleRemapper(ChunkTransportState *transportStates,
		int16 motNodeID,
		int16 targetRoute);

extern void DirectPutRxBufferTCP(ChunkTransportState *transportStates, int motNodeID, int route);
extern uint32 GetActiveMotionConnsTCP(void);
#endif
