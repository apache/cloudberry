/*-------------------------------------------------------------------------
 *
 * ic_tcp.h
 *	  Motion IPC TCP implements.
 *
 * IDENTIFICATION
 *	    contrib/interconnect/tcp/ic_tcp.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef IC_TCP_INTERFACE_H
#define IC_TCP_INTERFACE_H

#include "postgres.h"

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


/*
 * Registration message
 *
 * Upon making a connection, the sender sends a registration message to
 * identify itself to the receiver.  A lot of the fields are just there
 * for validity checking.
 */
typedef struct RegisterMessage
{
	int32		msgBytes;
	int32		recvSliceIndex;
	int32		sendSliceIndex;
	int32		srcContentId;
	int32		srcListenerPort;
	int32		srcPid;
	int32		srcSessionId;
	int32		srcCommandCount;
}			RegisterMessage;

extern int	GetMaxTupleChunkSizeTCP(void);

extern int32 GetListenPortTCP(void);

extern void InitMotionTCP(int *listenerSocketFd, int32 *listenerPort);
extern void TeardownTCPInterconnect(ChunkTransportState * transportStates, bool hasErrors);

extern void InitMotionIPCLayerTCP(void);
extern void CleanUpMotionIPCLayerTCP(void);

extern void WaitInterconnectQuitTCP(void);

extern void TeardownInterconnectTCP(ChunkTransportState * transportStates,
									bool hasErrors);

extern void SetupInterconnectTCP(EState *estate);

extern void DeregisterReadInterestTCP(ChunkTransportState * transportStates,
									  int motNodeID,
									  int srcRoute,
									  const char *reason);

extern bool SendChunkTCP(ChunkTransportState * transportStates, ChunkTransportStateEntry * pEntry,
						 MotionConn * conn, TupleChunkListItem tcItem, int16 motionId);
extern void SendEOSTCP(ChunkTransportState * transportStates,
					   int motNodeID, TupleChunkListItem tcItem);
extern void SendStopMessageTCP(ChunkTransportState * transportStates, int16 motNodeID);

extern TupleChunkListItem RecvTupleChunkFromAnyTCP(ChunkTransportState * transportStates,
												   int16 motNodeID,
												   int16 *srcRoute);

extern TupleChunkListItem RecvTupleChunkFromTCP(ChunkTransportState * transportStates,
												int16 motNodeID,
												int16 srcRoute);

extern TupleChunkListItem RecvTupleChunkTCP(MotionConn * conn, ChunkTransportState * transportStates);

#endif // IC_TCP_INTERFACE_H
