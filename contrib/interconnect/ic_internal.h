/*-------------------------------------------------------------------------
 *
 * ic_internal.h
 *	  Motion IPC Layer.
 *
 * IDENTIFICATION
 *		contrib/interconnect/ic_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INTER_CONNECT_INTERNAL_H
#define INTER_CONNECT_INTERNAL_H
#include <stdint.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/queue.h>
#include <pthread.h>

#include "tcp/ic_tcp.h"
#include "udp/ic_udpifc.h"
#include "cdb/cdbinterconnect.h"

#define CONTAINER_OF(ptr, type, member) \
	({ \
		const typeof( ((type *)0)->member ) *__member_ptr = (ptr); \
		(type *)( (char *)__member_ptr - offsetof(type,member) ); \
	})

typedef enum MotionConnState
{
	mcsNull,
	mcsAccepted,
	mcsSetupOutgoingConnection,
	mcsConnecting,
	mcsRecvRegMsg,
	mcsSendRegMsg,
	mcsStarted,
	mcsEosSent
}			MotionConnState;

struct udp_send_vars
{
	/* send sequence variables */
	uint32_t snd_una;		/* send unacknoledged */
	uint32_t snd_wnd;		/* send window (unscaled) */

	/* retransmission timeout variables */
	uint8_t nrtx;			/* number of retransmission */
	uint8_t max_nrtx;		/* max number of retransmission */
	uint32_t rto;			/* retransmission timeout */
	uint32_t ts_rto;		/* timestamp for retransmission timeout */

	/* congestion control variables */
	uint32_t cwnd;				/* congestion window */
	uint32_t ssthresh;			/* slow start threshold */

	TAILQ_ENTRY(MotionConnUDP) send_link;
	TAILQ_ENTRY(MotionConnUDP) timer_link;		/* timer link (rto list) */

};

/*
 * Structure used for keeping track of a pt-to-pt connection between two
 * Cdb Entities (either QE or QD).
 */
typedef struct MotionConn
{
	/* socket file descriptor. */
	int			sockfd;

	/* pointer to the data buffer. */
	uint8	   *pBuff;

	/* size of the message in the buffer, if any. */
	int32		msgSize;

	/* position of message inside of buffer, "cursor" pointer */
	uint8	   *msgPos;

	/*
	 * recv bytes: we can have more than one message/message fragment in recv
	 * queue at once
	 */
	int32		recvBytes;

	int			tupleCount;

	/*
	 * false means 1) received a stop message and has handled it. 2) received
	 * EOS message or sent out EOS message 3) received a QueryFinishPending
	 * notify and has handled it.
	 */
	bool		stillActive;

	/*
	 * used both by motion sender and motion receiver
	 *
	 * sender: true means receiver don't need to consume tuples any more,
	 * sender is also responsible to send stop message to its senders.
	 *
	 * receiver: true means have sent out a stop message to its senders. The
	 * stop message might be lost, stopRequested can also tell sender that no
	 * more data needed in the ack message.
	 */
	bool		stopRequested;

	MotionConnState state;

	struct CdbProcess *cdbProc;
	int			remoteContentId;
	char		remoteHostAndPort[128]; /* Numeric IP addresses should never
										 * be longer than about 50 chars, but
										 * play it safe */

	/*
	 * used by the sender.
	 *
	 * the typmod of last sent record type in current connection,
	 * if the connection is for broadcasting then we only check
	 * and update this attribute on connection 0.
	 */
	int32		 sent_record_typmod;

	/*
	 * used by the receiver.
	 *
	 * all the remap information.
	 */
	TupleRemapper *remapper;
}			MotionConn;

typedef struct MotionConnUDP
{
	struct MotionConn mConn;

	/* send side queue for packets to be sent */
	ICBufferList sndQueue;
	int			capacity;

	/* seq already sent */
	uint32		sentSeq;

	/* ack of this seq and packets with smaller seqs have been received */
	uint32		receivedAckSeq;

	/* packets with this seq or smaller seqs have been consumed */
	uint32		consumedSeq;

	uint64		rtt;
	uint64		dev;
	uint64		deadlockCheckBeginTime;

	ICBuffer   *curBuff;

	/*
	 * send side unacked packet queue. Since it is often accessed at the same
	 * time with unack queue ring, it is protected with unqck queue ring lock.
	 */
	ICBufferList unackQueue;

	uint16		route;

	struct icpkthdr conn_info;

	struct sockaddr_storage peer;	/* Allow for IPv4 or IPv6 */
	socklen_t	peer_len;		/* And remember the actual length */

	/* a queue of maximum length Gp_interconnect_queue_depth */
	int			pkt_q_capacity; /* max capacity of the queue */
	int			pkt_q_size;		/* number of packets in the queue */
	int			pkt_q_head;
	int			pkt_q_tail;
	uint8	  **pkt_q;

	uint64		stat_total_ack_time;
	uint64		stat_count_acks;
	uint64		stat_max_ack_time;
	uint64		stat_min_ack_time;
	uint64		stat_count_resent;
	uint64		stat_max_resent;
	uint64		stat_count_dropped;

	struct {
			uint32_t ts_rto;
			uint32_t rto;
			uint32_t srtt;
			uint32_t rttvar;
			uint32_t snd_una;
			uint16_t nrtx;
			uint16_t max_nrtx;
			uint32_t mss;
			uint32_t cwnd;
			uint32_t ssthresh;
			uint32_t fss;
			uint8_t loss_count;
			uint32_t mdev;
			uint32_t mdev_max;
			uint32_t rtt_seq;		/* sequence number to update rttvar */
			uint32_t ts_all_rto;
			bool karn_mode;
	} rttvar;
	
	uint8_t on_timewait_list;
	int16_t on_rto_idx;

	uint32_t snd_nxt;		/* send next */
	struct udp_send_vars sndvar;
}			MotionConnUDP;

typedef struct MotionConnTCP
{
	struct MotionConn mConn;

	uint64		wakeup_ms;

	char		localHostAndPort[128];
}			MotionConnTCP;

/*
 * Used to organize all of the information for a given motion node.
 */
typedef struct ChunkTransportStateEntry
{
	int			motNodeId;
	bool		valid;

	/* Connection array
	 *
	 * MUST pay attention: use getMotionConn to get MotionConn.
	 * must not use `->conns[index]` to get MotionConn. Because the struct
	 * MotionConn is a base structure for MotionConnTCP and
	 * MotionConnUDP. After connection setup, the `conns` will be fill
	 * with MotionConnUDP/MotionConnTCP, but the pointer still is
	 * MotionConn which should use `CONTAINER_OF` to get the real object.
	 */
	MotionConn *conns;
	int			numConns;

	int			scanStart;

	/*
	 * used for receiving. to select() from a set of interesting MotionConns
	 * to see when data is ready to be read.  When the incoming connections
	 * are established, read interest is turned on.  It is turned off when an
	 * EOS (End of Stream) message is read.
	 */
	mpp_fd_set	readSet;

	/* slice table entries */
	struct ExecSlice *sendSlice;
	struct ExecSlice *recvSlice;

}			ChunkTransportStateEntry;

typedef struct ChunkTransportStateEntryTCP
{
	ChunkTransportStateEntry entry;

	/* highest file descriptor in the readSet. */
	int			highReadSock;
}			ChunkTransportStateEntryTCP;

typedef struct ChunkTransportStateEntryUDP
{
	ChunkTransportStateEntry entry;

	/* setup info */
	int			txfd;
	int			txfd_family;
	unsigned short txport;

	bool		sendingEos;

	/* Statistics info for this motion on the interconnect level */
	uint64		stat_total_ack_time;
	uint64		stat_count_acks;
	uint64		stat_max_ack_time;
	uint64		stat_min_ack_time;
	uint64		stat_count_resent;
	uint64		stat_max_resent;
	uint64		stat_count_dropped;
}			ChunkTransportStateEntryUDP;

#define GetMotionConn(pEntry, MotionConnType, offset, cconn) \
	Assert((pEntry) != NULL);		\
	if (offset >= 0 && offset < (pEntry)->numConns) { \
		*(cconn) = &((MotionConnType *)(pEntry)->conns)[offset].mConn;\
	} else { \
		ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR), \
							errmsg("Interconnect Error: Unexpected Motion conn offset: %ld (size %d). This means" \
								   " a motion conn that wasn't setup is requesting interconnect" \
								   " resources.", (unsigned long int)(offset), (pEntry)->numConns))); \
	}

#define getMotionConn(pEntry, offset, cconn) \
	do { \
		if (CurrentMotionIPCLayer->ic_type == INTERCONNECT_TYPE_TCP || \
			CurrentMotionIPCLayer->ic_type == INTERCONNECT_TYPE_PROXY) { \
			GetMotionConn(pEntry, MotionConnTCP, offset, cconn) \
		} else if (CurrentMotionIPCLayer->ic_type == INTERCONNECT_TYPE_UDPIFC) { \
			GetMotionConn(pEntry, MotionConnUDP, offset, cconn) \
		}  else { \
			Assert(false); \
		} \
		Assert(*cconn); \
	} while (0)

#define allocMotionConns(pEntry) \
	do { \
		Assert((pEntry) != NULL);		\
		if (CurrentMotionIPCLayer->ic_type == INTERCONNECT_TYPE_TCP || \
			CurrentMotionIPCLayer->ic_type == INTERCONNECT_TYPE_PROXY) { \
			(pEntry)->conns = palloc0((pEntry)->numConns * sizeof(MotionConnTCP)); \
		} else if (CurrentMotionIPCLayer->ic_type == INTERCONNECT_TYPE_UDPIFC) { \
			(pEntry)->conns = palloc0((pEntry)->numConns * sizeof(MotionConnUDP)); \
		}  else { \
			Assert(false); \
		} \
	} while (0)
/*
 * Must used getChunkTransportState/getChunkTransportStateNoValid to fill ChunkTransportStateEntry
 * Cause is a ChunkTransportStateEntry array in ChunkTransportState
 */
#define GetChunkTransportState(transportState, ChunkTransportStateEntryType, motNodeID, ppEntry) \
		Assert((transportState) != NULL);		\
		if ((motNodeID) > 0 &&					\
			(transportState) &&					 \
			(motNodeID) <= (transportState)->size &&					\
			((ChunkTransportStateEntryType *)(transportState)->states)[(motNodeID)-1].entry.motNodeId == (motNodeID) && \
			((ChunkTransportStateEntryType *)(transportState)->states)[(motNodeID)-1].entry.valid)				\
		{ \
			*(ppEntry) = &((ChunkTransportStateEntryType *)(transportState)->states)[(motNodeID) - 1].entry;	\
		} \
		else \
		{ \
			ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR), \
							errmsg("Interconnect Error: Unexpected Motion Node Id: %d (size %d). This means" \
								   " a motion node that wasn't setup is requesting interconnect" \
								   " resources.", (motNodeID), (transportState)->size))); \
			/* not reached */ \
		}

#define getChunkTransportState(transportState, motNodeID, ppEntry) \
	do { \
		if (CurrentMotionIPCLayer->ic_type == INTERCONNECT_TYPE_TCP || \
			CurrentMotionIPCLayer->ic_type == INTERCONNECT_TYPE_PROXY) { \
			GetChunkTransportState(transportState, ChunkTransportStateEntryTCP,motNodeID, ppEntry) \
		} else if (CurrentMotionIPCLayer->ic_type == INTERCONNECT_TYPE_UDPIFC) { \
			GetChunkTransportState(transportState, ChunkTransportStateEntryUDP,motNodeID, ppEntry) \
		}  else { \
			Assert(false); \
		}\
	} while (0)

#define GetChunkTransportStateNoValid(transportState, ChunkTransportStateEntryType, motNodeID, ppEntry) \
		Assert((transportState) != NULL);		\
		if ((motNodeID) > 0 &&					\
			(transportState) &&					 \
			(motNodeID) <= (transportState)->size)				\
		{ \
			*(ppEntry) = &((ChunkTransportStateEntryType *)(transportState)->states)[(motNodeID) - 1].entry;	\
		} \
		else \
		{ \
			ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR), \
							errmsg("Interconnect Error: Unexpected Motion Node Id: %d (size %d). This means" \
								   " a motion node that wasn't setup is requesting interconnect" \
								   " resources.", (motNodeID), (transportState)->size))); \
			/* not reached */ \
		}

#define getChunkTransportStateNoValid(transportState, motNodeID, ppEntry) \
	do { \
		if (CurrentMotionIPCLayer->ic_type == INTERCONNECT_TYPE_TCP || \
			CurrentMotionIPCLayer->ic_type == INTERCONNECT_TYPE_PROXY) { \
			GetChunkTransportStateNoValid(transportState, ChunkTransportStateEntryTCP,motNodeID, ppEntry) \
		} else if (CurrentMotionIPCLayer->ic_type == INTERCONNECT_TYPE_UDPIFC) { \
			GetChunkTransportStateNoValid(transportState, ChunkTransportStateEntryUDP,motNodeID, ppEntry) \
		}  else { \
			Assert(false); \
		} \
	} while (0)

#endif // INTER_CONNECT_INTERNAL_H
