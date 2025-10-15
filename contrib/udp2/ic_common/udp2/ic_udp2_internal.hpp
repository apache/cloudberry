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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * ic_udp2_internal.hpp
 *
 * IDENTIFICATION
 *    contrib/udp2/ic_common/udp2/ic_udp2_internal.hpp
 *
 *-------------------------------------------------------------------------
 */
#ifndef IC_UDP2_INTERNAL_HPP
#define IC_UDP2_INTERNAL_HPP

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <atomic>
#include <cstdarg>
#include <vector>
#include <map>

#include <cassert>
#include <cstring>
#include <fcntl.h>
#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <netdb.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "ic_udp2.hpp"
#include "ic_utility.hpp"

namespace {

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
} MotionConnState;

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

	ICCdbProcess *cdbProc;
	int			remoteContentId;
	char		remoteHostAndPort[128]; /* Numeric IP addresses should never
										 * be longer than about 50 chars, but
										 * play it safe */

	void *opaque_data;

	/*
	 * used by the sender.
	 *
	 * the typmod of last sent record type in current connection,
	 * if the connection is for broadcasting then we only check
	 * and update this attribute on connection 0.
	 */
	int32 sent_record_typmod;

} MotionConn;

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

	/* slice table entries */
	struct ICExecSlice *sendSlice;
	struct ICExecSlice *recvSlice;

} ChunkTransportStateEntry;

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
	int32		icId;

	int32		recvSliceIndex;
	int32		sendSliceIndex;
	int32		srcContentId;
	int32		dstContentId;

	/* MPP-6042: add CRC field */
	uint32		crc;

	/* packet specific info */
	int32		flags;
	uint32		len;

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
} icpkthdr;

typedef struct ICBuffer ICBuffer;
typedef struct ICBufferLink ICBufferLink;

typedef enum ICBufferListType
{
	ICBufferListType_Primary,
	ICBufferListType_Secondary,
	ICBufferListType_UNDEFINED
} ICBufferListType;

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
struct ICBufferList
{
	int			len;
	ICBufferListType type;		/* primary or secondary */

	ICBufferLink head;

#if defined(USE_ASSERT_CHECKING) || defined(AMS_VERBOSE_LOGGING)
	void icBufferListLog();
#endif

#ifdef USE_ASSERT_CHECKING
	void icBufferListCheck(const char *prefix);
#endif

	void init(ICBufferListType type);
	void destroy();

	bool is_head(ICBufferLink *link);
	int  length();
	ICBufferLink* first();

	ICBuffer* append(ICBuffer *buf);
	ICBuffer* remove(ICBuffer *buf);
	ICBuffer* pop();

	void release(bool inExpirationQueue);

	void dump_to_file(FILE *ofile);
};

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
	int32		nRetry;
	int32		unackQueueRingSlot;

	/* real data */
	icpkthdr	pkt[0];
};

static inline void*
ic_malloc(size_t size)
{
	return malloc(size);
}

static inline void*
ic_malloc0(size_t size)
{
	void *rs = ic_malloc(size);
	if (rs)
		memset(rs, 0, size);
	return rs;
}

static inline void
ic_free(void *p)
{
	free(p);
}

static inline void
ic_free_clean(void **p)
{
	ic_free(*p);
	*p = NULL;
}

static inline void
ic_usleep(long microsec)
{
	if (microsec > 0)
	{
		struct timeval delay;

		delay.tv_sec = microsec / 1000000L;
		delay.tv_usec = microsec % 1000000L;
		(void) select(0, NULL, NULL, NULL, &delay);
	}
}

/*
 * Put socket into nonblock mode.
 * Returns true on success, false on failure.
 */
static inline bool
ic_set_noblock(int sock)
{
	int			flags;

	flags = fcntl(sock, F_GETFL);
	if (flags < 0)
		return false;
	if (fcntl(sock, F_SETFL, (flags | O_NONBLOCK)) == -1)
		return false;
	return true;
}

/* ic_atomic_xxx */
typedef struct ic_atomic_uint32
{
	volatile uint32 value;
} ic_atomic_uint32;

static inline void
ic_atomic_init_u32(volatile ic_atomic_uint32 *ptr, uint32 val)
{
	ptr->value = val;
}

static inline uint32
ic_atomic_read_u32(volatile ic_atomic_uint32 *ptr)
{
	return ptr->value;
}

static inline void
ic_atomic_write_u32(volatile ic_atomic_uint32 *ptr, uint32 val)
{
	ptr->value = val;
}

static inline bool
ic_atomic_compare_exchange_u32(volatile ic_atomic_uint32 *ptr,
							   uint32 *expected, uint32 newval)
{
	/* FIXME: we can probably use a lower consistency model */
	return __atomic_compare_exchange_n(&ptr->value, expected, newval, false,
									   __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

static inline uint32
ic_atomic_add_fetch_u32(volatile ic_atomic_uint32 *ptr, int32 add_)
{
	return __sync_fetch_and_add(&ptr->value, add_) + add_;
}

static inline uint32
ic_bswap32(uint32 x)
{
	return
		((x << 24) & 0xff000000) |
		((x << 8) & 0x00ff0000) |
		((x >> 8) & 0x0000ff00) |
		((x >> 24) & 0x000000ff);
}

#define MAX_TRY (11)
#define TIMEOUT(try) ((try) < MAX_TRY ? (timeoutArray[(try)]) : (timeoutArray[MAX_TRY]))

#define USECS_PER_SECOND 1000000
#define MSECS_PER_SECOND 1000

/* 1/4 sec in msec */
#define RX_THREAD_POLL_TIMEOUT (250)

/*
 * Flags definitions for flag-field of UDP-messages
 *
 * We use bit operations to test these, flags are powers of two only
 */
#define UDPIC_FLAGS_RECEIVER_TO_SENDER  (1)
#define UDPIC_FLAGS_ACK					(2)
#define UDPIC_FLAGS_STOP				(4)
#define UDPIC_FLAGS_EOS					(8)
#define UDPIC_FLAGS_NAK					(16)
#define UDPIC_FLAGS_DISORDER    		(32)
#define UDPIC_FLAGS_DUPLICATE   		(64)
#define UDPIC_FLAGS_CAPACITY    		(128)

#define UDPIC_MIN_BUF_SIZE (128 * 1024)

/*
 * ConnHtabBin
 *
 * A connection hash table bin.
 *
 */
struct UDPConn;
typedef struct ConnHtabBin ConnHtabBin;
struct ConnHtabBin
{
	UDPConn *conn;
	struct ConnHtabBin *next;
};

/*
 * ConnHashTable
 *
 * Connection hash table definition.
 *
 */
typedef struct ConnHashTable ConnHashTable;
struct ConnHashTable
{
	ConnHtabBin **table;
	int			size;

	bool init();
	bool add(UDPConn *conn);
	UDPConn *find(icpkthdr *hdr);
	void destroy();
	void remove(UDPConn *conn);
};

#define CONN_HASH_VALUE(icpkt) ((uint32)((((icpkt)->srcPid ^ (icpkt)->dstPid)) + (icpkt)->dstContentId))
#define CONN_HASH_MATCH(a, b) (((a)->motNodeId == (b)->motNodeId && \
								(a)->dstContentId == (b)->dstContentId && \
								(a)->srcContentId == (b)->srcContentId && \
								(a)->recvSliceIndex == (b)->recvSliceIndex && \
								(a)->sendSliceIndex == (b)->sendSliceIndex && \
								(a)->srcPid == (b)->srcPid &&			\
								(a)->dstPid == (b)->dstPid && (a)->icId == (b)->icId))

/*
 * CursorICHistoryEntry
 *
 * The definition of cursor IC history entry.
 */
typedef struct CursorICHistoryEntry CursorICHistoryEntry;
struct CursorICHistoryEntry
{
	/* Interconnect instance id. */
	uint32		icId;

	/* Command id. */
	uint32		cid;

	/*
	 * Interconnect instance status. state 1 (value 1): interconnect is setup
	 * state 0 (value 0): interconnect was torn down.
	 */
	uint8		status;

	/* Next entry. */
	CursorICHistoryEntry *next;

	CursorICHistoryEntry(uint32 aicId, uint32 acid):
		icId(aicId), cid(acid),status(1){}
};

/*
 * CursorICHistoryTable
 *
 * Cursor IC history table. It is a small hash table.
 */
typedef struct CursorICHistoryTable CursorICHistoryTable;
struct CursorICHistoryTable
{
	uint32		size;
	uint32		count;
	CursorICHistoryEntry **table;

	void init() {
		count = 0;
		size = session_param.Gp_interconnect_cursor_ic_table_size;
		table = (CursorICHistoryEntry **)ic_malloc0(sizeof(CursorICHistoryEntry *) * size);
	}

	void add(uint32 icId, uint32 cid) {
		uint32		index = icId % size;
		CursorICHistoryEntry *p = new CursorICHistoryEntry(icId, cid);

		p->next = this->table[index];
		this->table[index] = p;
		this->count++;

		LOG(DEBUG2, "add icid %d cid %d status %d", p->icId, p->cid, p->status);

		return;
	}

	/*
	 * state 1 (value 1): interconnect is setup
	 * state 0 (value 0): interconnect was torn down.
	 */
	void update(uint32 icId, uint8 status) {
		for (CursorICHistoryEntry *p = table[icId % size]; p; p = p->next) {
			if (p->icId == icId) {
				p->status = status;
				return;
			}
		}
	}

	CursorICHistoryEntry* get(uint32 icId) {
		for (CursorICHistoryEntry *p = table[icId % size]; p; p = p->next) {
			if (p->icId == icId)
				return p;
		}
		return NULL;
	}

	void purge() {
		for (uint8 index = 0; index < size; index++) {
			while (table[index]) {
				CursorICHistoryEntry *trash = table[index];
				table[index] = trash->next;
				delete trash;
			}
		}
	}

	void prune(uint32 icId);
};

/*
 * Synchronization timeout values
 *
 * MAIN_THREAD_COND_TIMEOUT - 1/4 second
 */
#define MAIN_THREAD_COND_TIMEOUT_MS (250)

/*
 *  Used for synchronization between main thread (receiver) and background thread.
 *
 */
typedef struct ThreadWaitingState ThreadWaitingState;
struct ThreadWaitingState
{
	bool		waiting;
	int			waitingNode;
	int			waitingRoute;
	int			reachRoute;

	/* main_thread_waiting_query is needed to disambiguate for cursors */
	int			waitingQuery;

	void reset() {
		waiting      = false;
		waitingNode  = -1;
		waitingRoute = ANY_ROUTE;
		reachRoute   = ANY_ROUTE;
		waitingQuery = -1;
	}

	void set(int motNodeId, int route, int icId) {
		waiting      = true;
		waitingNode  = motNodeId;
		waitingRoute = route;
		reachRoute   = ANY_ROUTE;
		waitingQuery = icId;
	}
};

/*
 * ReceiveControlInfo
 *
 * The related control information for receiving data packets.
 * Main thread (Receiver) and background thread use the information in
 * this data structure to handle data packets.
 *
 */
typedef struct ReceiveControlInfo ReceiveControlInfo;
struct ReceiveControlInfo
{
	/* Main thread waiting state. */
	ThreadWaitingState mainWaitingState;

	/*
	 * Buffers used to assemble disorder messages at receiver side.
	 */
	icpkthdr   *disorderBuffer;

	/* The last interconnect instance id which is torn down. */
	int32		lastTornIcId;

	/* Cursor history table. */
	CursorICHistoryTable cursorHistoryTable;

	/*
	 * Last distributed transaction id when SetupUDPInterconnect is called.
	 * Coupled with cursorHistoryTable, it is used to handle multiple
	 * concurrent cursor cases.
	 */
	DistributedTransactionId lastDXatId;
};

/*
 * RxBufferPool
 *
 * Receive thread buffer pool definition. The implementation of
 * receive side buffer pool is different from send side buffer pool.
 * It is because receive side buffer pool needs a ring buffer to
 * easily implement disorder message handling logic.
 */

typedef struct RxBufferPool RxBufferPool;
struct RxBufferPool
{
	/* The max number of buffers we can get from this pool. */
	int			maxCount;

	/* The number of allocated buffers */
	int			count;

	/* The list of free buffers. */
	char	   *freeList;

	void      put(icpkthdr *buf);
	void      release(icpkthdr *buf);
	icpkthdr* get();
	icpkthdr* get_free();
};

/*
 * SendBufferPool
 *
 * The send side buffer pool definition.
 *
 */
typedef struct SendBufferPool SendBufferPool;
struct SendBufferPool
{
	/* The maximal number of buffers sender can use. */
	int			maxCount;

	/* The number of buffers sender already used. */
	int			count;

	/* The free buffer list at the sender side. */
	ICBufferList freeList;

	void init();
	void clean();
	ICBuffer* get(UDPConn *conn);
};

/*
 * SendControlInfo
 *
 * The related control information for sending data packets and handling acks.
 * Main thread use the information in this data structure to do ack handling
 * and congestion control.
 *
 */
typedef struct SendControlInfo SendControlInfo;
struct SendControlInfo
{
	/* The buffer used for accepting acks */
	icpkthdr   *ackBuffer;

	/* congestion window */
	float		cwnd;

	/* minimal congestion control window */
	float		minCwnd;

	/* slow start threshold */
	float		ssthresh;

};

/*
 * ICGlobalControlInfo
 *
 * Some shared control information that is used by main thread (senders, receivers, or both)
 * and the background thread.
 *
 */
typedef struct ICGlobalControlInfo ICGlobalControlInfo;
struct ICGlobalControlInfo
{
	/* The background thread handle. */
	pthread_t	threadHandle;

	/* Keep the udp socket buffer size used. */
	uint32		socketSendBufferSize;
	uint32		socketRecvBufferSize;

	uint64		lastExpirationCheckTime;
	uint64		lastDeadlockCheckTime;

	/* Used to decide whether to retransmit for capacity based FC. */
	uint64		lastPacketSendTime;

	/*
	 * Lock and latch for coordination between main thread and
	 * background thread. It protects the shared data between the two threads
	 * (the connHtab, rx buffer pool and the mainWaitingState etc.).
	 */
	pthread_mutex_t lock;

	/* Am I a sender? */
	bool		isSender;

	/* Flag showing whether the thread is created. */
	bool		threadCreated;

	/* Error number. Actually int but we do not have ic_atomic_int32. */
	ic_atomic_uint32 eno;

	/*
	 * Global connection htab for both sending connections and receiving
	 * connections. Protected by the lock in this data structure.
	 */
	ConnHashTable connHtab;

	/* The connection htab used to cache future packets. */
	ConnHashTable startupCacheHtab;

	/* Used by main thread to ask the background thread to exit. */
	ic_atomic_uint32 shutdown;

	/*Serialization
	 * Used by ic thread in the QE to identify the current serving ic instance
	 * and handle the mismatch packets. It is not used by QD because QD may have
	 * cursors, QD may receive packets for open the cursors with lower instance
	 * id, QD use cursorHistoryTable to handle packets mismatch.
	 */
	int32		ic_instance_id;
};

/*
 * Macro for unack queue ring, round trip time (RTT) and expiration period (RTO)
 *
 * UNACK_QUEUE_RING_SLOTS_NUM - the number of slots in the unack queue ring.
 *                              this value should be greater than or equal to 2.
 * TIMER_SPAN                 - timer period in us
 * TIMER_CHECKING_PERIOD      - timer checking period in us
 * UNACK_QUEUE_RING_LENGTH    - the whole time span of the unack queue ring
 * DEFAULT_RTT                - default rtt in us.
 * MIN_RTT                    - min rtt in us
 * MAX_RTT                    - max rtt in us
 * RTT_SHIFT_COEFFICIENT      - coefficient for RTT computation
 *
 * DEFAULT_DEV                - default round trip standard deviation
 * MAX_DEV                    - max dev
 * DEV_SHIFT_COEFFICIENT      - coefficient for DEV computation
 *
 * MAX_EXPIRATION_PERIOD      - max expiration period in us
 * MIN_EXPIRATION_PERIOD      - min expiration period in us
 * MAX_TIME_NO_TIMER_CHECKING - max time without checking timer
 * DEADLOCK_CHECKING_TIME     - deadlock checking time
 *
 * MAX_SEQS_IN_DISORDER_ACK   - max number of sequences that can be transmitted in a
 *                              disordered packet ack.
 *
 *
 * Considerations on the settings of the values:
 *
 * TIMER_SPAN and UNACK_QUEUE_RING_SLOTS_NUM define the ring period.
 * Currently, it is UNACK_QUEUE_RING_LENGTH (default 10 seconds).
 *
 * The definition of UNACK_QUEUE_RING_LENGTH is quite related to the size of
 * sender side buffer and the size we may resend in a burst for an expiration event
 * (which may overwhelm switch or OS if it is too large).
 * Thus, we do not want to send too much data in a single expiration event. Here, a
 * relatively large UNACK_QUEUE_RING_SLOTS_NUM value is used to avoid that.
 *
 * If the sender side buffer is X (MB), then on each slot,
 * there are about X/UNACK_QUEUE_RING_SLOTS_NUM. Even we have a very large sender buffer,
 * for example, 100MB, there is about 96M/2000 = 50K per slot.
 * This is fine for the OS (with buffer 2M for each socket generally) and switch.
 *
 * Note that even when the buffers are not evenly distributed in the ring and there are some packet
 * losses, the congestion control mechanism, the disorder and duplicate packet handling logic will
 * assure the number of outstanding buffers (in unack queues) to be not very large.
 *
 * MIN_RTT/MAX_RTT/DEFAULT_RTT/MIN_EXPIRATION_PERIOD/MAX_EXPIRATION_PERIOD gives some heuristic values about
 * the computation of RTT and expiration period. RTT and expiration period (RTO) are not
 * constant for various kinds of hardware and workloads. Thus, they are computed dynamically.
 * But we also want to bound the values of RTT and MAX_EXPIRATION_PERIOD. It is
 * because there are some faults that may make RTT a very abnormal value. Thus, RTT and
 * expiration period are upper and lower bounded.
 *
 * MAX_SEQS_IN_DISORDER_ACK should be smaller than (MIN_PACKET_SIZE - sizeof(icpkthdr))/sizeof(uint32).
 * It is due to the limitation of the ack receive buffer size.
 *
 */
#define UNACK_QUEUE_RING_SLOTS_NUM (2000)
#define TIMER_SPAN (session_param.Gp_interconnect_timer_period * 1000ULL)	/* default: 5ms */
#define TIMER_CHECKING_PERIOD (session_param.Gp_interconnect_timer_checking_period)	/* default: 20ms */
#define UNACK_QUEUE_RING_LENGTH (UNACK_QUEUE_RING_SLOTS_NUM * TIMER_SPAN)

#define DEFAULT_RTT (session_param.Gp_interconnect_default_rtt * 1000)	/* default: 20ms */
#define MIN_RTT (100)			/* 0.1ms */
#define MAX_RTT (200 * 1000)	/* 200ms */
#define RTT_SHIFT_COEFFICIENT (3)	/* RTT_COEFFICIENT 1/8 (0.125) */

#define DEFAULT_DEV (0)
#define MIN_DEV MIN_RTT
#define MAX_DEV MAX_RTT
#define DEV_SHIFT_COEFFICIENT (2)	/* DEV_COEFFICIENT 1/4 (0.25) */

#define MAX_EXPIRATION_PERIOD (1000 * 1000) /* 1s */
#define MIN_EXPIRATION_PERIOD (session_param.Gp_interconnect_min_rto * 1000)	/* default: 20ms */

#define MAX_TIME_NO_TIMER_CHECKING (50 * 1000)	/* 50ms */
#define DEADLOCK_CHECKING_TIME  (512 * 1000)	/* 512ms */

#define MAX_SEQS_IN_DISORDER_ACK (4)

/*
 * UnackQueueRing
 *
 * An unacked queue ring is used to decide which packet is expired in constant time.
 *
 * Each slot of the ring represents a fixed time span, for example 1ms, and
 * each slot has a associated buffer list/queue which contains the packets
 * which will expire in the time span.
 *
 * If the current time pointer (time t) points to slot 1,
 * then slot 2 represents the time span from t + 1ms to t + 2ms.
 * When we check whether there are some packets expired, we start from the last
 * current time recorded, and resend all the packets in the queue
 * until we reach the slot that the updated current time points to.
 *
 */
typedef struct UnackQueueRing UnackQueueRing;
struct UnackQueueRing
{
	/* save the current time when we check the time wheel for expiration */
	uint64		currentTime;

	/* the slot index corresponding to current time */
	int			idx;

	/* the number of outstanding packets in unack queue ring */
	int			numOutStanding;

	/*
	 * the number of outstanding packets that use the shared bandwidth in the
	 * congestion window.
	 */
	int			numSharedOutStanding;

	/* time slots */
	ICBufferList slots[UNACK_QUEUE_RING_SLOTS_NUM];
};

/*
 * AckSendParam
 *
 * The parameters for ack sending.
 */
typedef struct AckSendParam
{
	/* header for the ack */
	icpkthdr	msg;

	/* peer address for the ack */
	struct sockaddr_storage peer;
	socklen_t	peer_len;
} AckSendParam;

/*
 * ICStatistics
 *
 * A structure keeping various statistics about interconnect internal.
 *
 * Note that the statistics for ic are not accurate for multiple cursor case on QD.
 *
 * totalRecvQueueSize        - receive queue size sum when main thread is trying to get a packet.
 * recvQueueSizeCountingTime - counting times when computing totalRecvQueueSize.
 * totalCapacity             - the capacity sum when packets are tried to be sent.
 * capacityCountingTime      - counting times used to compute totalCapacity.
 * totalBuffers              - total buffers available when sending packets.
 * bufferCountingTime        - counting times when compute totalBuffers.
 * activeConnectionsNum      - the number of active connections.
 * retransmits               - the number of packet retransmits.
 * mismatchNum               - the number of mismatched packets received.
 * crcErrors                 - the number of crc errors.
 * sndPktNum                 - the number of packets sent by sender.
 * recvPktNum                - the number of packets received by receiver.
 * disorderedPktNum          - disordered packet number.
 * duplicatedPktNum          - duplicate packet number.
 * recvAckNum                - the number of Acks received.
 * statusQueryMsgNum         - the number of status query messages sent.
 *
 */
typedef struct ICStatistics
{
	uint64		totalRecvQueueSize;
	uint64		recvQueueSizeCountingTime;
	uint64		totalCapacity;
	uint64		capacityCountingTime;
	uint64		totalBuffers;
	uint64		bufferCountingTime;
	uint32		activeConnectionsNum;
	int32		retransmits;
	int32		startupCachedPktNum;
	int32		mismatchNum;
	int32		crcErrors;
	int32		sndPktNum;
	int32		recvPktNum;
	int32		disorderedPktNum;
	int32		duplicatedPktNum;
	int32		recvAckNum;
	int32		statusQueryMsgNum;
} ICStatistics;

struct TransportEntry;

/*
 * Structure used for keeping track of a pt-to-pt connection between two
 * Cdb Entities (either QE or QD).
 */
struct UDPConn : public MotionConn
{
public:
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
	uint32		pkt_q_capacity; /* max capacity of the queue */
	uint32		pkt_q_size;		/* number of packets in the queue */
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

	TransportEntry *entry_;

public:
	UDPConn(TransportEntry *entry);

	void GetDataInBuf(GetDataLenInPacket getLen, DataBlock *data);
	void ReleaseBuffer(AckSendParam *param);

	void setAckParam(AckSendParam *param, int32 flags, uint32 seq, uint32 extraSeq);
	void sendAck(int32 flags, uint32 seq, uint32 extraSeq);
	void sendDisorderAck(uint32 seq, uint32 extraSeq, uint32 lostPktCnt);
	void sendStatusQueryMessage(uint32 seq);

	uint64 computeExpirationPeriod(uint32 retry);

	void freeDisorderedPackets();
	void prepareRxConnForRead();
	void DeactiveConn();

	void handleAckedPacket(ICBuffer *buf, uint64 now);
	void prepareXmit();
	void sendOnce(icpkthdr *pkt);
	void handleStop();
	void sendBuffers();

	void handleDisorderPacket(int pos, uint32 tailSeq, icpkthdr *pkt);
	bool handleAckForDisorderPkt(icpkthdr *pkt);
	bool handleAckForDuplicatePkt(icpkthdr *pkt);
	int computeTimeout(int retry);

	void Send(DataBlock *data);

	void checkDeadlock();
	void checkExceptions(int retry, int timeout);

	void updateRetransmitStatistics();
	void checkExpirationCapacityFC(int timeout);

	static void checkNetworkTimeout(ICBuffer *buf, uint64 now, bool *networkTimeoutIsLogged);
	static void checkExpiration(ICChunkTransportState *transportStates, uint64 now);

	static void sendAckWithParam(AckSendParam *param);
	static void sendControlMessage(icpkthdr *pkt, int fd, struct sockaddr *addr, socklen_t peerLen);
};


/*
 * Used to organize all of the information for a given motion node.
 */
struct CChunkTransportStateEntry
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

	/* slice table entries */
	struct ICExecSlice *sendSlice;
	struct ICExecSlice *recvSlice;
};

class CChunkTransportStateImpl;

class TransportEntry : public CChunkTransportStateEntry
{
public:
	static std::unique_ptr<TransportEntry>
		MakeRecvEntry(CChunkTransportStateImpl *state, int icid, ICExecSlice *sendSlice, ICExecSlice *recvSlice);

	static std::unique_ptr<TransportEntry>
		MakeSendEntry(CChunkTransportStateImpl *state, int icid, ICExecSlice *sendSlice, ICExecSlice *recvSlice);

	TransportEntry(CChunkTransportStateImpl *state, int motNodeID, int numConns, ICExecSlice *sendSlice, ICExecSlice *recvSlice);

	UDPConn* GetConn(int index);

	void aggregateStatistics();

	bool handleAcks();
	void handleStopMsgs();

	bool pollAcks(int timeout);

	void dumpConnections(const char *fname);

	bool SendData(int16 targetRoute, DataBlock *pblocks, int num, bool broadcast);
	void Broadcast(DataBlock *data, int *inactiveCountPtr);
	void Send(int16 targetRoute, DataBlock *db, bool broadcast, int *recount);

	void RecvAny(int16 *srcRoute, GetDataLenInPacket getLen, DataBlock *data);
	void RecvRoute(int16 srcRoute, GetDataLenInPacket getLen, DataBlock *data);
	void receiveChunksUDPIFC(int16 *srcRoute, UDPConn *conn, GetDataLenInPacket getLen, DataBlock *data);

public:
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

	std::vector<std::unique_ptr<UDPConn>> conns_;
	CChunkTransportStateImpl *state;
};


class CChunkTransportStateImpl : public CChunkTransportState
{
public:
	CChunkTransportStateImpl(ICSliceTable *sliceTable);

	static ICChunkTransportState* SetupUDP(ICSliceTable *sliceTable, SessionMotionLayerIPCParam *param);
	void TeardownUDP(bool hasErrors);

	void RecvRoute(int16 motNodeID, int16 srcRoute, GetDataLenInPacket getLen, DataBlock *data);
	void RecvAny(int16 motNodeID, int16 *srcRoute, GetDataLenInPacket getLen, DataBlock *data);
	void SendStop(int16 motNodeID);
	void ReleaseAndAck(int motNodeID, int route);
	void DeactiveRoute(int motNodeID, int srcRoute, const char *reason);

	void SendEOS(int motNodeID, DataBlock *data);
	bool SendData(int16 motNodeID, int16 targetRoute, DataBlock *pblocks, int num, bool broadcast);
	void GetFreeSpace(int16 motNodeID, int16 targetRoute, BufferBlock *b);
	void ReduceFreeSpace(int16 motNodeID, int16 targetRoute, int length);

	void* GetOpaqueDataInConn(int16 motNodeID, int16 targetRoute);
	int32* GetSentRecordTypmodInConn(int16 motNodeID, int16 targetRoute);

	int  GetConnNum(int motNodeID);

	TransportEntry* GetEntry(int motNodeID, bool checkValid);

	static CChunkTransportState **GetTransportState();

	/* APIs for vector engine */
	void NotifyQuit();
	void SetVectorEngineAsUser();

private:
	void checkMotNodeID(int sendMotNodeID);
	void CreateRecvEntries(ICSliceTable *sliceTable);
	void CreateSendEntries(ICSliceTable *sliceTable);
	void DestroyRecvEntries(bool *isReceiver);
	void DestroySendEntries();
	static ICChunkTransportState* setup(ICSliceTable *sliceTable);
	void teardown(bool hasErrors);

	std::vector<std::unique_ptr<TransportEntry>>  entries_;

	static CChunkTransportState *state_;
};

} // namespace

#endif  /* IC_UDP2_INTERNAL_HPP */