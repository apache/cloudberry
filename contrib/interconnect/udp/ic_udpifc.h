/*-------------------------------------------------------------------------
 *
 * ic_udpifc.h
 *	  Motion IPC UDP implements.
 *
 * IDENTIFICATION
 *	    contrib/interconnect/udp/ic_udpifc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef IC_UDP_INTERFACE_H
#define IC_UDP_INTERFACE_H

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

typedef struct icpkthdr
{
	int32		motNodeId;

	/*
	 * three pairs which seem useful for identifying packets.
	 *
	 * MPP-4194: It turns out that these can cause collisions; but the high
	 * bit (1<<31) of the dstListener port is now used for disambiguation with
	 * mirrors.
	 */
	int32		srcPid;
	int32		srcListenerPort;

	int32		dstPid;
	int32		dstListenerPort;

	int32		sessionId;
	uint32		icId;

	int32		recvSliceIndex;
	int32		sendSliceIndex;
	int32		srcContentId;
	int32		dstContentId;

	/* MPP-6042: add CRC field */
	uint32		crc;

	/* packet specific info */
	int32		flags;
	int32		len;

	/*
     * The usage of seq and extraSeq field
     * a) In a normal DATA packet
     *    seq      -> the data packet sequence number
     *    extraSeq -> not used
     * b) In a normal ACK message (UDPIC_FLAGS_ACK | UDPIC_FLAGS_CAPACITY)
     *    seq      -> the largest seq of the continuously cached packets
     *                sometimes, it is special, for exampke, conn req ack, mismatch ack.
     *    extraSeq -> the largest seq of the consumed packets
     * c) In a start race NAK message (UPDIC_FLAGS_NAK)
     *    seq      -> the seq from the pkt
     *    extraSeq -> the extraSeq from the pkt
     * d) In a DISORDER message (UDPIC_FLAGS_DISORDER)
     *    seq      -> packet sequence number that triggers the disorder message
     *    extraSeq -> the largest seq of the received packets
     * e) In a DUPLICATE message (UDPIC_FLAGS_DUPLICATE)
     *    seq      -> packet sequence number that triggers the duplicate message
     *    extraSeq -> the largest seq of the continuously cached packets
     * f) In a stop messege (UDPIC_FLAGS_STOP | UDPIC_FLAGS_ACK | UDPIC_FLAGS_CAPACITY)
     *    seq      -> the largest seq of the continuously cached packets
     *    extraSeq -> the largest seq of the continuously cached packets
     *
     *
     * NOTE that: EOS/STOP flags are often saved in conn_info structure of a connection.
     *			  It is possible for them to be sent together with other flags.
     *
     */
	uint32		seq;
	uint32		extraSeq;
	uint64_t send_time;
	uint64_t recv_time;
	uint8_t retry_times;
}			icpkthdr;

typedef struct ICBuffer ICBuffer;
typedef struct ICBufferLink ICBufferLink;

typedef enum ICBufferListType
{
	ICBufferListType_Primary,
	ICBufferListType_Secondary,
	ICBufferListType_UNDEFINED
}			ICBufferListType;

struct ICBufferLink
{
	ICBufferLink *next;
	ICBufferLink *prev;
};

/*
 * ICBufferList
 * 		ic buffer list data structure.
 *
 * There are two kinds of lists. The first kind of list uses the primary next/prev pointers.
 * And the second kind uses the secondary next/prev pointers.
 */
typedef struct ICBufferList
{
	int			length;
	ICBufferListType type;		/* primary or secondary */

	ICBufferLink head;
}			ICBufferList;


#define GET_ICBUFFER_FROM_PRIMARY(ptr) CONTAINER_OF(ptr, ICBuffer, primary)
#define GET_ICBUFFER_FROM_SECONDARY(ptr) CONTAINER_OF(ptr, ICBuffer, secondary)

/*
 * ICBuffer
 * 		interconnect buffer data structure.
 *
 * In some cases, an ICBuffer may exists in two lists/queues,
 * thus it has two sets of pointers. For example, an ICBuffer
 * can exist in an unack queue and an expiration queue at the same time.
 *
 * It is important to get the ICBuffer address when we iterate a list of
 * ICBuffers through primary/secondary links. The Macro GET_ICBUFFER_FROM_PRIMARY
 * and GET_ICBUFFER_FROM_SECONDARY are for this purpose.
 *
 */
struct ICBuffer
{
	/* primary next and prev pointers */
	ICBufferLink primary;

	/* secondary next and prev pointers */
	ICBufferLink secondary;

	/* connection that this buffer belongs to */
	MotionConn *conn;

	/*
	 * Three fields for expiration processing
	 *
	 * sentTime - the time this buffer was sent nRetry   - the number of send
	 * retries unackQueueRingSlot - unack queue ring slot index
	 */
	uint64		sentTime;
	uint32		nRetry;
	int32		unackQueueRingSlot;

	/* real data */
	icpkthdr	pkt[0];
};

extern void logChunkParseDetails(MotionConn * conn, uint32 ic_instance_id);

extern int	GetMaxTupleChunkSizeUDP(void);

extern int32 GetListenPortUDP(void);

extern void InitMotionIPCLayerUDP(void);
extern void CleanUpMotionLayerIPCUDP(void);

extern void WaitInterconnectQuitUDPIFC(void);

extern void SetupInterconnectUDP(EState *estate);
extern void TeardownInterconnectUDP(ChunkTransportState * transportStates, bool hasErrors);

extern void DeregisterReadInterestUDP(ChunkTransportState * transportStates,
									  int motNodeID,
									  int srcRoute,
									  const char *reason);

extern TupleChunkListItem
RecvTupleChunkUDPIFC(MotionConn * conn, ChunkTransportState * transportStates);

extern bool SendChunkUDPIFC(ChunkTransportState * transportStates, ChunkTransportStateEntry * pChunkEntry,
							MotionConn * conn, TupleChunkListItem tcItem, int16 motionId);
extern void SendEOSUDPIFC(ChunkTransportState * transportStates,
						  int motNodeID, TupleChunkListItem tcItem);
extern void SendStopMessageUDPIFC(ChunkTransportState * transportStates, int16 motNodeID);

extern TupleChunkListItem RecvTupleChunkFromAnyUDPIFC(ChunkTransportState * transportStates,
													  int16 motNodeID,
													  int16 *srcRoute);

extern TupleChunkListItem RecvTupleChunkFromUDPIFC(ChunkTransportState * transportStates,
												   int16 motNodeID,
												   int16 srcRoute);
extern uint32 GetActiveMotionConnsUDPIFC(void);

void		MlPutRxBufferIFC(ChunkTransportState * transportStates, int motNodeID, int route);

/* debug function for udpifc */
extern void dumpICBufferList(ICBufferList * list, const char *fname);
extern void dumpUnackQueueRing(const char *fname);
extern void dumpConnections(ChunkTransportStateEntry * pEntry, const char *fname);

#endif // IC_UDP_INTERFACE_H
