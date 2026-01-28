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
 * ic_udp2.cpp
 *
 * IDENTIFICATION
 *    contrib/udp2/ic_common/udp2/ic_udp2.cpp
 *
 *               +--------------+
 *               |  ic_types.h  |
 *               +--------------+
 *                  /         \
 *       +--------------+  +---------------+
 *       | C interface  |  | C++ interface |
 *       |  ic_udp2.h   |  |  ic_udp2.hpp  |
 *       +--------------+  +---------------+
 *                   \         /
 *            +----------------------+
 *            |     C++ implement    |
 *            |  ic_udp2_internal.hpp|
 *            |  ic_faultinjection.h |
 *            |  ic_udp2.cpp         |
 *            +----------------------+
 *-------------------------------------------------------------------------
 */
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>
#include <atomic>
#include <cstdarg>

/*
 * interface header files
 */
#ifdef __cplusplus
extern "C" {
#endif

#include "ic_types.h"
#include "ic_udp2.h"

#ifdef __cplusplus
}
#endif

#include "ic_udp2.hpp"

/*
 * internal header files
 */
#include "ic_utility.hpp"
#include "ic_udp2_internal.hpp"
#include "ic_faultinjection.h"

/*
 * Hints to the compiler about the likelihood of a branch. Both likely() and
 * unlikely() return the boolean value of the contained expression.
 *
 * These should only be used sparingly, in very hot code paths. It's very easy
 * to mis-estimate likelihoods.
 */
#if __GNUC__ >= 3
#define likely(x)	__builtin_expect((x) != 0, 1)
#define unlikely(x) __builtin_expect((x) != 0, 0)
#else
#define likely(x)	((x) != 0)
#define unlikely(x) ((x) != 0)
#endif

static int timeoutArray[] =
{
	1,
	1,
	2,
	4,
	8,
	16,
	32,
	64,
	128,
	256,
	512,
	512							/* MAX_TRY */
};

/*
 * Main thread (Receiver) and background thread use the information in
 * this data structure to handle data packets.
 */
static ReceiveControlInfo rx_control_info;

/*
 * The buffer pool used for keeping data packets.
 *
 * maxCount is set to 1 to make sure there is always a buffer
 * for picking packets from OS buffer.
 */
static RxBufferPool rx_buffer_pool = {1, 0, NULL};

/*
 * The sender side buffer pool.
 */
static SendBufferPool snd_buffer_pool;

/*
 * Main thread use the information in this data structure to do ack handling
 * and congestion control.
 */
static SendControlInfo snd_control_info;

/*
 * Shared control information that is used by senders, receivers and background thread.
 */
static ICGlobalControlInfo ic_control_info;

/*
 * All connections in a process share this unack queue ring instance.
 */
static UnackQueueRing unack_queue_ring = {0};

static int	ICSenderSocket = -1;
static int32 ICSenderPort = 0;
static int	ICSenderFamily = 0;

/* Statistics for UDP interconnect. */
static ICStatistics ic_statistics;

/* Cached sockaddr of the listening udp socket */
static struct sockaddr_storage udp_dummy_packet_sockaddr;

/* UDP listen fd */
static int			UDP_listenerFd = -1;

/* UDP listen port */
static int32 udp_listener_port = 0;

static std::mutex mtx;
static std::condition_variable cv;

CChunkTransportState *CChunkTransportStateImpl::state_ = nullptr;

static struct mudp_manager mudp;

/*
 * Identity the user of ic module by vector_engine_is_user:
 * "false" means PG executor, "true" means Arrow executor.
 */
static thread_local bool vector_engine_is_user = false;
static thread_local bool thread_quit = false;

#define CHECK_QUIT_FLAG() \
	do { \
		if (thread_quit) { \
			throw ICException("received thread quit flag.", __FILE__, __LINE__); \
		} \
	} while(0)

#define CHECK_INTERRUPTS(state) \
	do { \
		if (vector_engine_is_user) { \
			CHECK_QUIT_FLAG(); \
		} else if (global_param.checkInterruptsCallback) { \
			global_param.checkInterruptsCallback((state)->teardownActive); \
		} \
	} while(0)

#define CHECK_CANCEL(state) \
	do { \
		if (vector_engine_is_user) { \
			CHECK_QUIT_FLAG(); \
		} else if (global_param.checkCancelOnQDCallback) { \
			global_param.checkCancelOnQDCallback(state); \
		} \
	} while(0)

#define CHECK_POSTMASTER_ALIVE() \
	do { \
		if (vector_engine_is_user) { \
			CHECK_QUIT_FLAG(); \
		} else if (global_param.checkPostmasterIsAliveCallback && !global_param.checkPostmasterIsAliveCallback()) { \
			throw ICFatalException("FATAL, interconnect failed to send chunks, Postmaster is not alive.", __FILE__, __LINE__); \
		} \
	} while(0)

/*=========================================================================
 * STATIC FUNCTIONS declarations
 */

/* Background thread error handling functions. */
static void checkRxThreadError(void);
static void setRxThreadError(int eno);
static void resetRxThreadError(void);

static uint32 setUDPSocketBufferSize(int ic_socket, int buffer_type);
static void setupOutgoingUDPConnection(int icid, TransportEntry *pChunkEntry, UDPConn *conn);

/* ICBufferList functions. */
static inline void icBufferListInitHeadLink(ICBufferLink *link);

static inline void InitMotionUDPIFC(int *listenerSocketFd, int32 *listenerPort);
static inline void CleanupMotionUDPIFC(void);

static bool dispatcherAYT(void);
static void checkQDConnectionAlive(void);

static void *rxThreadFunc(void *arg);

static void putIntoUnackQueueRing(UnackQueueRing *uqr, ICBuffer *buf, uint64 expTime, uint64 now);

static bool cacheFuturePacket(icpkthdr *pkt, struct sockaddr_storage *peer, int peer_len);
static void cleanupStartupCache(void);
static void handleCachedPackets(void);

static uint64 getCurrentTime(void);
static void initMutex(pthread_mutex_t *mutex);

static inline void logPkt(const char *prefix, icpkthdr *pkt);

static void ConvertToIPv4MappedAddr(struct sockaddr_storage *sockaddr, socklen_t *o_len);
#if defined(__darwin__)
#define    s6_addr32 __u6_addr.__u6_addr32
static void ConvertIPv6WildcardToLoopback(struct sockaddr_storage* dest);
#endif

static void setupUDPListeningSocket(int *listenerSocketFd, int32 *listenerPort,
                           int *txFamily, struct sockaddr_storage *listenerSockaddr);
static void getSockAddr(struct sockaddr_storage *peer, socklen_t *peer_len, const char *listenerAddr, int listenerPort);
static void SendDummyPacket(void);
static bool handleDataPacket(UDPConn *conn, icpkthdr *pkt, struct sockaddr_storage *peer, socklen_t *peerlen, AckSendParam *param, bool *wakeup_mainthread);
static bool handleMismatch(icpkthdr *pkt, struct sockaddr_storage *peer, int peer_len);
static void initUnackQueueRing(UnackQueueRing *uqr);

static ssize_t sendtoWithRetry(int socket, const void *message, size_t length, int flags, const struct sockaddr *dest_addr, socklen_t dest_len, int retry, const char *errDetail);

static char *format_sockaddr_udp(struct sockaddr_storage *sa, char *buf, size_t len);

static void initUdpManager(mudp_manager_t mptr);

static char* flags2txt(uint32 pkt_flags);

static const char* flags_text[] =
	{"recv2send", "ack", "stop", "eos", "nak", "disorder", "duplicate", "capacity"};

static char*
flags2txt(uint32 pkt_flags)
{
	thread_local static char flags[64];

	char *p = flags;
	*p = '\0';
	int bytes = 0;
	for (size_t i = 0; i < sizeof(flags_text)/sizeof(flags_text[0]); ++i) {
		if (pkt_flags & (1 << i))
			bytes += snprintf(p + bytes, 64, "%s | ", flags_text[i]);
	}

	if (bytes > 0)
		bytes -= 3;
	*(p + bytes) = '\0';

	return flags;
}

/*
 * CursorICHistoryTable::prune
 * 		Prune entries in the hash table.
 */
void
CursorICHistoryTable::prune(uint32 icId) {
	for (uint32 index = 0; index < size; index++) {
		CursorICHistoryEntry *p = table[index], *q = NULL;
		while (p) {
			/* remove an entry if it is older than the prune-point */
			if (p->icId < icId) {
				if (!q)
					table[index] = p->next;
				else
					q->next = p->next;

				/* set up next loop */
				CursorICHistoryEntry *trash = p;
				p = trash->next;
				ic_free(trash);

				count--;
			} else {
				q = p;
				p = p->next;
			}
		}
	}
}

#ifdef TRANSFER_PROTOCOL_STATS
typedef enum TransProtoEvent
{
	TPE_DATA_PKT_SEND,
	TPE_ACK_PKT_QUERY
} TransProtoEvent;

typedef struct TransProtoStatEntry TransProtoStatEntry;
struct TransProtoStatEntry
{
	TransProtoStatEntry *next;

	/* Basic information */
	uint32		time;
	TransProtoEvent event;
	int			dstPid;
	uint32		seq;

	/* more attributes can be added on demand. */

	/*
	 * float	cwnd;
	 * int		capacity;
	 */
};

typedef struct TransProtoStats TransProtoStats;
struct TransProtoStats
{
	std::mutex lock;
	TransProtoStatEntry *head;
	TransProtoStatEntry *tail;
	uint64		count;
	uint64		startTime;

	void init();
	void update(TransProtoEvent event, icpkthdr *pkt);
	void dump();
};

static TransProtoStats trans_proto_stats =
{
	{}, NULL, NULL, 0
};

/*
 * init
 * 		Initialize the transport protocol states data structures.
 */
void
TransProtoStats::init()
{
	std::lock_guard<std::mutex> guard(this->lock);

	while (this->head) {
		TransProtoStatEntry *cur = this->head;
		this->head = this->head->next;
		ic_free(cur);
		this->count--;
	}

	this->head = NULL;
	this->tail = NULL;
	this->count = 0;
	this->startTime = getCurrentTime();
}

void
TransProtoStats::update(TransProtoEvent event, icpkthdr *pkt)
{
	/* Add to list */
	TransProtoStatEntry *entry = (TransProtoStatEntry *) ic_malloc(sizeof(TransProtoStatEntry));
	if (!entry)
		return;

	memset(entry, 0, sizeof(*entry));

	/* change the list */
	std::lock_guard<std::mutex> guard(this->lock);
	if (this->count == 0) {
		/* 1st element */
		this->head = entry;
		this->tail = entry;
	} else {
		this->tail->next = entry;
		this->tail = entry;
	}
	this->count++;

	entry->time = getCurrentTime() - this->startTime;
	entry->event = event;
	entry->dstPid = pkt->dstPid;
	entry->seq = pkt->seq;

	/*
	 * Other attributes can be added on demand new->cwnd =
	 * snd_control_info.cwnd; new->capacity = conn->capacity;
	 */
}

void
TransProtoStats::dump()
{
	char		tmpbuf[32];

	snprintf(tmpbuf, 32, "%d.%lu.txt", global_param.MyProcPid, getCurrentTime());
	FILE	   *ofile = fopen(tmpbuf, "w+");

	std::lock_guard<std::mutex> guard(this->lock);
	while (this->head)
	{
		TransProtoStatEntry *cur = NULL;

		cur = this->head;
		this->head = this->head->next;

		fprintf(ofile, "time %d event %d seq %d destpid %d\n", cur->time, cur->event, cur->seq, cur->dstPid);
		ic_free(cur);
		this->count--;
	}

	this->tail = NULL;
	fclose(ofile);
}

#endif							/* TRANSFER_PROTOCOL_STATS */

/*
 * initConnHashTable
 * 		Initialize a connection hash table.
 */
bool
ConnHashTable::init()
{
	this->size = global_param.Gp_role == GP_ROLE_DISPATCH_IC ?
				 (global_param.segment_number * 2) : global_param.ic_htab_size;
	Assert(this->size > 0);

	this->table = (struct ConnHtabBin **) ic_malloc(this->size * sizeof(struct ConnHtabBin *));
	if (this->table == NULL)
		return false;

	for (int i = 0; i < this->size; i++)
		this->table[i] = NULL;

	return true;
}

/*
 * connAddHash
 * 		Add a connection to the hash table
 *
 * Note: we want to add a connection to the hashtable if it isn't
 * already there ... so we just have to check the pointer values -- no
 * need to use CONN_HASH_MATCH() at all!
 */
bool
ConnHashTable::add(UDPConn *conn)
{
	uint32 hashcode = CONN_HASH_VALUE(&conn->conn_info) % this->size;

	/*
	 * check for collision -- if we already have an entry for this connection,
	 * don't add another one.
	 */
	for (struct ConnHtabBin *bin = this->table[hashcode]; bin != NULL; bin = bin->next)
	{
		if (bin->conn == conn)
		{
			LOG(DEBUG5, "ConnHashTable::add(): duplicate ?! node %d route %d", conn->conn_info.motNodeId, conn->route);
			return true;		/* false *only* indicates memory-alloc
								 * failure. */
		}
	}

	struct ConnHtabBin *newbin = (struct ConnHtabBin *) ic_malloc(sizeof(struct ConnHtabBin));
	if (newbin == NULL)
		return false;

	newbin->conn = conn;
	newbin->next = this->table[hashcode];
	this->table[hashcode] = newbin;

	ic_statistics.activeConnectionsNum++;

	return true;
}

/*
 * remove
 * 		Delete a connection from the hash table
 *
 * Note: we want to remove a connection from the hashtable if it is
 * there ... so we just have to check the pointer values -- no need to
 * use CONN_HASH_MATCH() at all!
 */
void
ConnHashTable::remove(UDPConn *conn)
{
	uint32		hashcode;
	struct ConnHtabBin *c,
			   *p,
			   *trash;

	hashcode = CONN_HASH_VALUE(&conn->conn_info) % this->size;

	c = this->table[hashcode];

	/* find entry */
	p = NULL;
	while (c != NULL)
	{
		/* found ? */
		if (c->conn == conn)
			break;

		p = c;
		c = c->next;
	}

	/* not found ? */
	if (c == NULL)
	{
		return;
	}

	/* found the connection, remove from the chain. */
	trash = c;

	if (p == NULL)
		this->table[hashcode] = c->next;
	else
		p->next = c->next;

	ic_free(trash);

	ic_statistics.activeConnectionsNum--;

	return;
}

/*
 * findConnByHeader
 * 		Find the corresponding connection given a pkt header information.
 *
 * With the new mirroring scheme, the interconnect is no longer involved:
 * we don't have to disambiguate anymore.
 *
 * NOTE: the icpkthdr field dstListenerPort is used for disambiguation.
 * on receivers it may not match the actual port (it may have an extra bit
 * set (1<<31)).
 */
UDPConn *
ConnHashTable::find(icpkthdr *hdr) {

	uint32 hashcode = CONN_HASH_VALUE(hdr) % this->size;
	for (struct ConnHtabBin *bin = this->table[hashcode]; bin != NULL; bin = bin->next) {
		UDPConn *conn = bin->conn;

		if (CONN_HASH_MATCH(&conn->conn_info, hdr)) {
			UDPConn *ret = conn;
			if (IC_DEBUG5 >= session_param.log_min_messages)
				LOG(DEBUG5, "ConnHashTable::find: found. route %d state %d hashcode %d conn %p",
					conn->route, ret->state, hashcode, ret);

			return ret;
		}
	}

	if (IC_DEBUG5 >= session_param.log_min_messages)
		LOG(DEBUG5, "ConnHashTable::find: not found! (hdr->srcPid %d hdr->srcContentId %d "
					"hdr->dstContentId %d hdr->dstPid %d sess(%d:%d) cmd(%d:%d)) hashcode %d",
					hdr->srcPid, hdr->srcContentId, hdr->dstContentId, hdr->dstPid, hdr->sessionId,
					session_param.gp_session_id, hdr->icId, ic_control_info.ic_instance_id, hashcode);

	return NULL;
}

void
ConnHashTable::destroy() {
	for (int i = 0; i < this->size; i++) {
		while (this->table[i] != NULL) {
			struct ConnHtabBin *trash = this->table[i];
			this->table[i] = trash->next;
			ic_free(trash);
		}
	}

	ic_free(this->table);
	this->table = NULL;
	this->size = 0;
}

/*
 * icBufferListInitHeadLink
 * 		Initialize the pointers in the head link to point to itself.
 */
static inline void
icBufferListInitHeadLink(ICBufferLink *link)
{
	link->next = link->prev = link;
}


#if defined(USE_ASSERT_CHECKING) || defined(AMS_VERBOSE_LOGGING)

/*
 * icBufferListLog
 * 		Log the buffer list.
 */
void
ICBufferList::icBufferListLog()
{
	LOG(INFO, "Length %d, type %d headptr %p", this->len, this->type, &this->head);

	ICBufferLink *bufLink = this->head.next;

	int			len = this->len;
	int			i = 0;

	while (bufLink != &this->head && len > 0)
	{
		ICBuffer   *buf = (this->type == ICBufferListType_Primary ? GET_ICBUFFER_FROM_PRIMARY(bufLink)
						   : GET_ICBUFFER_FROM_SECONDARY(bufLink));

		LOG(INFO, "Node %d, linkptr %p", i++, bufLink);

		logPkt("from list", buf->pkt);
		bufLink = bufLink->next;
		len--;
	}
}
#endif

#ifdef USE_ASSERT_CHECKING
/*
 * icBufferListCheck
 * 		Buffer list sanity check.
 */
void
ICBufferList::icBufferListCheck(const char *prefix)
{
	int len = this->len;
	ICBufferLink *link = this->head.next;

	if (len < 0)
	{
		LOG(LOG_ERROR, "ICBufferList ERROR %s: list length %d < 0 ", prefix, this->length());
		goto error;
	}

	if (len == 0 && (this->head.prev != this->head.next && this->head.prev != &this->head))
	{
		LOG(LOG_ERROR, "ICBufferList ERROR %s: length is 0, &list->head %p, prev %p, next %p",
					   prefix, &this->head, this->head.prev, this->head.next);
		this->icBufferListLog();
		goto error;
	}

	while (len > 0)
	{
		link = link->next;
		len--;
	}

	if (link != &this->head)
	{
		LOG(LOG_ERROR, "ICBufferList ERROR: %s len %d", prefix, this->len);
		this->icBufferListLog();
		goto error;
	}

	return;

error:
	LOG(INFO, "wait for 120s and then abort.");
	ic_usleep(120000000);
	abort();
}
#endif

/*
 * ICBufferList::init
 * 		Initialize the buffer list with the given type.
 */
void
ICBufferList::init(ICBufferListType atype)
{
	Assert(atype == ICBufferListType_Primary|| atype == ICBufferListType_Secondary);

	type = atype;
	len  = 0;

	icBufferListInitHeadLink(&head);

#ifdef USE_ASSERT_CHECKING
	this->icBufferListCheck("ICBufferList::init");
#endif
}

/*
 * ICBufferList::is_head
 * 		Return whether the given link is the head link of the list.
 *
 * 	This function is often used as the end condition of an iteration of the list.
 */
bool
ICBufferList::is_head(ICBufferLink *link)
{
#ifdef USE_ASSERT_CHECKING
	this->icBufferListCheck("ICBufferList::is_head");
#endif
	return (link == &head);
}

/*
 * ICBufferList::first
 * 		Return the first link after the head link.
 *
 * 	Note that the head link is a pseudo link used to only to ease the operations of the link list.
 * 	If the list only contains the head link, this function will return the head link.
 */
ICBufferLink *
ICBufferList::first()
{
#ifdef USE_ASSERT_CHECKING
	this->icBufferListCheck("ICBufferList::first");
#endif
	return head.next;
}

/*
 * ICBufferList::length
 * 		Get the list length.
 */
int
ICBufferList::length()
{
	return len;
}

/*
 * ICBufferList::delete
 *		Remove an buffer from the buffer list and return the buffer.
 */
ICBuffer *
ICBufferList::remove(ICBuffer *buf)
{
#ifdef USE_ASSERT_CHECKING
	this->icBufferListCheck("ICBufferList::delete");
#endif

	ICBufferLink *bufLink = NULL;

	bufLink = (this->type == ICBufferListType_Primary ? &buf->primary : &buf->secondary);

	bufLink->prev->next = bufLink->next;
	bufLink->next->prev = bufLink->prev;

	len--;

	return buf;
}

/*
 * ICBufferList::pop
 * 		Remove the head buffer from the list.
 */
ICBuffer *
ICBufferList::pop()
{
	ICBuffer   *buf = NULL;
	ICBufferLink *bufLink = NULL;

#ifdef USE_ASSERT_CHECKING
	this->icBufferListCheck("ICBufferList::pop");
#endif

	if (this->len == 0)
		return NULL;

	bufLink = this->first();
	buf = (this->type == ICBufferListType_Primary ? GET_ICBUFFER_FROM_PRIMARY(bufLink)
		   : GET_ICBUFFER_FROM_SECONDARY(bufLink));

	bufLink->prev->next = bufLink->next;
	bufLink->next->prev = bufLink->prev;

	this->len--;

	return buf;
}

/*
 * ICBufferList::free
 * 		Free all the buffers in the list.
 */
void
ICBufferList::destroy()
{
	ICBuffer   *buf = NULL;

#ifdef USE_ASSERT_CHECKING
	this->icBufferListCheck("ICBufferList::free");
#endif

	while ((buf = this->pop()) != NULL)
		ic_free(buf);
}

/*
 * ICBufferList::append
 * 		Append a buffer to the tail of a double-link list.
 */
ICBuffer *
ICBufferList::append(ICBuffer *buf)
{
	Assert(buf);

#ifdef USE_ASSERT_CHECKING
	this->icBufferListCheck("ICBufferList::append");
#endif

	ICBufferLink *bufLink = NULL;

	bufLink = (this->type == ICBufferListType_Primary ? &buf->primary : &buf->secondary);

	bufLink->prev = this->head.prev;
	bufLink->next = &this->head;

	this->head.prev->next = bufLink;
	this->head.prev = bufLink;

	this->len++;

	return buf;
}

/*
 * ICBufferList::return
 * 		Return the buffers in the list to the free buffer list.
 *
 * If the buf is also in an expiration queue, we also need to remove it from the expiration queue.
 *
 */
void
ICBufferList::release(bool inExpirationQueue)
{
#ifdef USE_ASSERT_CHECKING
	this->icBufferListCheck("ICBufferList::return");
#endif
	ICBuffer   *buf = NULL;

	while ((buf = this->pop()) != NULL)
	{
		if (inExpirationQueue)	/* the buf is in also in the expiration queue */
		{
			ICBufferList *alist = &unack_queue_ring.slots[buf->unackQueueRingSlot];
			buf = alist->remove(buf);
			unack_queue_ring.numOutStanding--;
			if (this->length() >= 1)
				unack_queue_ring.numSharedOutStanding--;
		}

		snd_buffer_pool.freeList.append(buf);
	}
}

#ifdef USE_ASSERT_CHECKING
/*
 * ICBufferList::dump_to_file
 * 		Dump a buffer list.
 */
void
ICBufferList::dump_to_file(FILE *ofile)
{
	this->icBufferListCheck("ICBufferList::dump_to_file");

	ICBufferLink *bufLink = this->head.next;

	int			len = this->len;
	int			i = 0;

	fprintf(ofile, "List Length %d\n", len);
	while (bufLink != &this->head && len > 0)
	{
		ICBuffer   *buf = (this->type == ICBufferListType_Primary ? GET_ICBUFFER_FROM_PRIMARY(bufLink)
						   : GET_ICBUFFER_FROM_SECONDARY(bufLink));

		fprintf(ofile, "Node %d, linkptr %p ", i++, bufLink);
		fprintf(ofile, "Packet Content [%s: seq %d extraSeq %d]: motNodeId %d, crc %d len %d "
				"srcContentId %d dstDesContentId %d "
				"srcPid %d dstPid %d "
				"srcListenerPort %d dstListernerPort %d "
				"sendSliceIndex %d recvSliceIndex %d "
				"sessionId %d icId %d "
				"flags %d\n",
				buf->pkt->flags & UDPIC_FLAGS_RECEIVER_TO_SENDER ? "ACK" : "DATA",
				buf->pkt->seq, buf->pkt->extraSeq, buf->pkt->motNodeId, buf->pkt->crc, buf->pkt->len,
				buf->pkt->srcContentId, buf->pkt->dstContentId,
				buf->pkt->srcPid, buf->pkt->dstPid,
				buf->pkt->srcListenerPort, buf->pkt->dstListenerPort,
				buf->pkt->sendSliceIndex, buf->pkt->recvSliceIndex,
				buf->pkt->sessionId, buf->pkt->icId,
				buf->pkt->flags);
		bufLink = bufLink->next;
		len--;
	}
}
#endif

/*
 * initUnackQueueRing
 *		Initialize an unack queue ring.
 *
 *	Align current time to a slot boundary and set current slot index (time pointer) to 0.
 */
static void
initUnackQueueRing(UnackQueueRing *uqr)
{
	int			i = 0;

	uqr->currentTime = 0;
	uqr->idx = 0;
	uqr->numOutStanding = 0;
	uqr->numSharedOutStanding = 0;

	for (; i < UNACK_QUEUE_RING_SLOTS_NUM; i++)
	{
		uqr->slots[i].init(ICBufferListType_Secondary);
	}

#ifdef TIMEOUT_Z
	uqr->retrans_count = 0;
	uqr->no_retrans_count = 0;
	uqr->time_difference = 0;
	uqr->min = 0;
	uqr->max = 0;
#endif
}

/*
 * RxBufferPool::get
 * 		Get a receive buffer.
 *
 * SHOULD BE CALLED WITH ic_control_info.lock *LOCKED*
 *
 * NOTE: This function MUST NOT contain elog or ereport statements.
 * elog is NOT thread-safe.  Developers should instead use something like:
 *
 * NOTE: In threads, we cannot use palloc/pfree, because it's not thread safe.
 */
icpkthdr *
RxBufferPool::get()
{
	icpkthdr   *ret = NULL;

#ifdef USE_ASSERT_CHECKING
	if (FINC_HAS_FAULT(FINC_RX_BUF_NULL) &&
		testmode_inject_fault(session_param.gp_udpic_fault_inject_percent))
		return NULL;
#endif

	do
	{
		if (this->freeList == NULL)
		{
			if (this->count > this->maxCount)
			{
				if (IC_DEBUG3 >= session_param.log_min_messages)
					LOG(DEBUG3, "Interconnect ran out of rx-buffers count/max %d/%d", this->count, this->maxCount);
				break;
			}

			/* malloc is used for thread safty. */
			ret = (icpkthdr *) ic_malloc(global_param.Gp_max_packet_size);

			/*
			 * Note: we return NULL if the malloc() fails -- and the
			 * background thread will set the error. Main thread will check
			 * the error, report it and start teardown.
			 */
			if (ret != NULL)
				this->count++;

			break;
		}

		/* we have buffers available in our freelist */
		ret = this->get_free();

	} while (0);

	return ret;
}

/*
 * RxBufferPool::put
 * 		Return a receive buffer to free list
 *
 *  SHOULD BE CALLED WITH ic_control_info.lock *LOCKED*
 */
void
RxBufferPool::put(icpkthdr *buf)
{
	/* return the buffer into the free list. */
	*(char **) buf = this->freeList;
	this->freeList = (char *)buf;
}

/*
 * RxBufferPool::get_free
 * 		Get a receive buffer from free list
 *
 * SHOULD BE CALLED WITH ic_control_info.lock *LOCKED*
 *
 * NOTE: This function MUST NOT contain elog or ereport statements.
 * elog is NOT thread-safe.  Developers should instead use something like:
 *
 * NOTE: In threads, we cannot use palloc/pfree, because it's not thread safe.
 */
icpkthdr *
RxBufferPool::get_free()
{
	icpkthdr   *buf = NULL;

	buf = (icpkthdr *) this->freeList;
	this->freeList = *(char **) (this->freeList);
	return buf;
}

/*
 * RxBufferPool::free
 * 		Free a receive buffer.
 *
 * NOTE: This function MUST NOT contain elog or ereport statements.
 * elog is NOT thread-safe.  Developers should instead use something like:
 *
 * NOTE: In threads, we cannot use palloc/pfree, because it's not thread safe.
 */
void
RxBufferPool::release(icpkthdr *buf)
{
	ic_free(buf);
	count--;
}

/*
 * init
 * 		Initialize the send buffer pool.
 *
 * The initial maxCount is set to 1 for gp_interconnect_snd_queue_depth = 1 case,
 * then there is at least an extra free buffer to send for that case.
 */
void
SendBufferPool::init()
{
	this->freeList.init(ICBufferListType_Primary);
	this->count = 0;
	this->maxCount = (session_param.Gp_interconnect_snd_queue_depth == 1 ? 1 : 0);
}

/*
 * clean
 * 		Clean the send buffer pool.
 */
void
SendBufferPool::clean()
{
	this->freeList.destroy();
	this->count = 0;
	this->maxCount = 0;
}

/*
 * get
 * 		Get a send buffer for a connection.
 *
 *  Different flow control mechanisms use different buffer management policies.
 *  Capacity based flow control uses per-connection buffer policy and Loss based
 *  flow control uses shared buffer policy.
 *
 * 	Return NULL when no free buffer available.
 */
ICBuffer *
SendBufferPool::get(UDPConn *conn)
{
	ICBuffer   *ret = NULL;

	ic_statistics.totalBuffers += (this->freeList.length() + this->maxCount - this->count);
	ic_statistics.bufferCountingTime++;

	/* Capacity based flow control does not use shared buffers */
	if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_CAPACITY_IC)
	{
		Assert(conn->unackQueue.length() + conn->sndQueue.length() <= session_param.Gp_interconnect_snd_queue_depth);
		if (conn->unackQueue.length() + conn->sndQueue.length() >= session_param.Gp_interconnect_snd_queue_depth)
			return NULL;
	}

	if (this->freeList.length() > 0)
	{
		return this->freeList.pop();
	}
	else
	{
		if (this->count < this->maxCount)
		{
			ret = (ICBuffer *) ic_malloc0(global_param.Gp_max_packet_size + sizeof(ICBuffer));
			this->count++;
			ret->conn = NULL;
			ret->nRetry = 0;
			icBufferListInitHeadLink(&ret->primary);
			icBufferListInitHeadLink(&ret->secondary);
			ret->unackQueueRingSlot = 0;
		}
		else
		{
			return NULL;
		}
	}

	return ret;
}

static struct rto_hashstore*
initRTOHashstore()
{
	int i;
	struct rto_hashstore* hs = (struct rto_hashstore*)ic_malloc(sizeof(struct rto_hashstore));

	for (i = 0; i < RTO_HASH; i++)
		TAILQ_INIT(&hs->rto_list[i]);

	TAILQ_INIT(&hs->rto_list[RTO_HASH]);

	return hs;
}

static void
initUdpManager(mudp_manager_t mudp)
{
	mudp->rto_store = initRTOHashstore();
	mudp->rto_list_cnt = 0;
	mudp->cur_ts = 0;
}

static inline void
addtoRTOList(mudp_manager_t mudp, UDPConn *cur_stream)
{
	if (!mudp->rto_list_cnt)
	{
		mudp->rto_store->rto_now_idx = 0;
		mudp->rto_store->rto_now_ts = cur_stream->sndvar.ts_rto;
	}

	if (cur_stream->on_rto_idx < 0 )
	{
		if (cur_stream->on_timewait_list)
			return;

		int diff = (int32_t)(cur_stream->sndvar.ts_rto - mudp->rto_store->rto_now_ts);
		if (diff < RTO_HASH)
		{
			int offset= (diff + mudp->rto_store->rto_now_idx) % RTO_HASH;
			cur_stream->on_rto_idx = offset;
			TAILQ_INSERT_TAIL(&(mudp->rto_store->rto_list[offset]),
					cur_stream, sndvar.timer_link);
		}
		else
		{
			cur_stream->on_rto_idx = RTO_HASH;
			TAILQ_INSERT_TAIL(&(mudp->rto_store->rto_list[RTO_HASH]),
					cur_stream, sndvar.timer_link);
		}
		mudp->rto_list_cnt++;
	}
}

static inline void
removeFromRTOList(mudp_manager_t mudp,
				UDPConn *cur_stream)
{
	if (cur_stream->on_rto_idx < 0)
		return;

	TAILQ_REMOVE(&mudp->rto_store->rto_list[cur_stream->on_rto_idx],
			cur_stream, sndvar.timer_link);
	cur_stream->on_rto_idx = -1;

	mudp->rto_list_cnt--;
}

static inline void
updateRetransmissionTimer(mudp_manager_t mudp,
				UDPConn *cur_stream,
				uint32_t cur_ts)
{
	cur_stream->sndvar.nrtx = 0;

	/* if in rto list, remove it */
	if (cur_stream->on_rto_idx >= 0)
		removeFromRTOList(mudp, cur_stream);

	/* Reset retransmission timeout */
	if (UDP_SEQ_GT(cur_stream->snd_nxt, cur_stream->sndvar.snd_una))
	{
		/* there are packets sent but not acked */
		/* update rto timestamp */
		cur_stream->sndvar.ts_rto = cur_ts + cur_stream->sndvar.rto;
		addtoRTOList(mudp, cur_stream);
	}

	if (cur_stream->on_rto_idx == -1)
	{
		cur_stream->sndvar.ts_rto = cur_ts + cur_stream->sndvar.rto;
		addtoRTOList(mudp, cur_stream);
	}
}

static int 
handleRTO(mudp_manager_t mudp,
				uint32_t cur_ts,
				UDPConn *cur_stream,
				ICChunkTransportState *transportStates,
				TransportEntry *pEntry,
				UDPConn *triggerConn)
{
	/* check for expiration */
	int                     count = 0;
	int                     retransmits = 0;
	UDPConn *currBuffConn = NULL;
	uint32_t now = cur_ts;

	Assert(unack_queue_ring.currentTime != 0);
	removeFromRTOList(mudp, cur_stream);

	while (now >= (unack_queue_ring.currentTime + TIMER_SPAN) && count++ < UNACK_QUEUE_RING_SLOTS_NUM)
	{
		/* expired, need to resend them */
		ICBuffer   *curBuf = NULL;

		while ((curBuf = unack_queue_ring.slots[unack_queue_ring.idx].pop()) != NULL)
		{
			curBuf->nRetry++;
			currBuffConn = static_cast<UDPConn*>(curBuf->conn);
			putIntoUnackQueueRing(
								&unack_queue_ring,
								curBuf,
								currBuffConn->computeExpirationPeriod(curBuf->nRetry), now);

#ifdef TRANSFER_PROTOCOL_STATS
			trans_proto_stats.update(TPE_DATA_PKT_SEND, curBuf->pkt);
#endif

			currBuffConn->sendOnce(curBuf->pkt);

			retransmits++;
			ic_statistics.retransmits++;
			currBuffConn->stat_count_resent++;
			currBuffConn->stat_max_resent = Max(currBuffConn->stat_max_resent, currBuffConn->stat_count_resent);
			UDPConn::checkNetworkTimeout(curBuf, now, &transportStates->networkTimeoutIsLogged);

#ifdef AMS_VERBOSE_LOGGING
			LOG(INFO, "RESEND pkt with seq %d (retry %d, rtt " UINT64_FORMAT ") to route %d",
				  curBuf->pkt->seq, curBuf->nRetry, curBuf->conn->rtt, curBuf->conn->route);
			logPkt("RESEND PKT in checkExpiration", curBuf->pkt);
#endif
		}

		unack_queue_ring.currentTime += TIMER_SPAN;
		unack_queue_ring.idx = (unack_queue_ring.idx + 1) % (UNACK_QUEUE_RING_SLOTS_NUM);
	}
	return 0;
}

static inline void
rearrangeRTOStore(mudp_manager_t mudp)
{
	UDPConn *walk, *next;
	struct rto_hashstore::rto_head* rto_list = &mudp->rto_store->rto_list[RTO_HASH];
	int cnt = 0;

	for (walk = TAILQ_FIRST(rto_list); walk != NULL; walk = next)
	{
		next = TAILQ_NEXT(walk, sndvar.timer_link);

		int diff = (int32_t)(mudp->rto_store->rto_now_ts - walk->sndvar.ts_rto);
		if (diff < RTO_HASH)
		{
			int offset = (diff + mudp->rto_store->rto_now_idx) % RTO_HASH;
			TAILQ_REMOVE(&mudp->rto_store->rto_list[RTO_HASH],
					walk, sndvar.timer_link);
			walk->on_rto_idx = offset;
			TAILQ_INSERT_TAIL(&(mudp->rto_store->rto_list[offset]),
					walk, sndvar.timer_link);
		}
		cnt++;
	}
}

static inline void
checkRtmTimeout(mudp_manager_t mudp,
				uint32_t cur_ts,
				int thresh,
				ICChunkTransportState *transportStates,
				TransportEntry *pEntry,
				UDPConn *triggerConn)
{
	UDPConn *walk, *next;
	struct rto_hashstore::rto_head* rto_list;
	int cnt;

	if (!mudp->rto_list_cnt)
		return;

	cnt = 0;

	while (1)
	{
		rto_list = &mudp->rto_store->rto_list[mudp->rto_store->rto_now_idx];
		if ((int32_t)(cur_ts - mudp->rto_store->rto_now_ts) < 0)
			break;

		for (walk = TAILQ_FIRST(rto_list); walk != NULL; walk = next)
		{
			if (++cnt > thresh)
				break;
			next = TAILQ_NEXT(walk, sndvar.timer_link);

			if (walk->on_rto_idx >= 0)
			{
				TAILQ_REMOVE(rto_list, walk, sndvar.timer_link);
				mudp->rto_list_cnt--;
				walk->on_rto_idx = -1;
				handleRTO(mudp, cur_ts, walk, transportStates, pEntry, triggerConn);
			}
		}

		if (cnt > thresh)
		{
			break;
		}
		else
		{
			mudp->rto_store->rto_now_idx = (mudp->rto_store->rto_now_idx + 1) % RTO_HASH;
			mudp->rto_store->rto_now_ts++;
			if (!(mudp->rto_store->rto_now_idx % 1000))
				rearrangeRTOStore(mudp);
		}

	}
}

/*
 * estimateRTT - Dynamically estimates the Round-Trip Time (RTT) and adjusts Retransmission Timeout (RTO)
 * 
 * This function implements a variant of the Jacobson/Karels algorithm for RTT estimation, adapted for UDP-based
 * motion control connections. It updates smoothed RTT (srtt), mean deviation (mdev), and RTO values based on 
 * newly measured RTT samples (mrtt). The RTO calculation ensures reliable data transmission over unreliable networks.
 *
 * Key Components:
 *   - srtt:   Smoothed Round-Trip Time (weighted average of historical RTT samples)
 *   - mdev:   Mean Deviation (measure of RTT variability)
 *   - rttvar: Adaptive RTT variation bound (used to clamp RTO updates)
 *   - rto:    Retransmission Timeout (dynamically adjusted based on srtt + rttvar)
 *
 * Algorithm Details:
 *   1. For the first RTT sample:
 *        srtt    = mrtt << 3   (scaled by 8 for fixed-point arithmetic)
 *        mdev    = mrtt << 1   (scaled by 2)
 *        rttvar  = max(mdev, rto_min)
 *   2. For subsequent samples:
 *        Delta   = mrtt - (srtt >> 3)  (difference between new sample and smoothed RTT)
 *        srtt   += Delta               (update srtt with 1/8 weight of new sample)
 *        Delta   = abs(Delta) - (mdev >> 2)
 *        mdev   += Delta               (update mdev with 1/4 weight)
 *   3. rttvar bounds the maximum RTT variation:
 *        If mdev > mdev_max, update mdev_max and rttvar
 *        On new ACKs (snd_una > rtt_seq), decay rttvar toward mdev_max
 *   4. Final RTO calculation:
 *        rto = (srtt >> 3) + rttvar   (clamped to RTO_MAX)
 *
 * Parameters:
 *   @mConn:  Parent motion connection context (container of MotionConnUDP)
 *   @mrtt:   Measured Round-Trip Time (in microseconds) for the latest packet
 *
 * Notes:
 *   - Designed for non-retransmitted packets to avoid sampling bias.
 *   - Uses fixed-point arithmetic to avoid floating-point operations.
 *   - Minimum RTO (rto_min) is set to 20ms (HZ/5/10, assuming HZ=100).
 *   - Critical for adaptive timeout control in UDP protocols where reliability is implemented at the application layer.
 *   - Thread-unsafe: Must be called in a synchronized context (e.g., packet processing loop).
 */
static inline void
estimateRTT(UDPConn *conn , uint32_t mrtt)
{
	/* This function should be called for not retransmitted packets */
	/* TODO: determine rto_min */

	long m = mrtt;
	uint32_t rto_min = UDP_RTO_MIN / 10;

	if (m == 0)
		m = 1;

	/*
	 * Special RTO optimization for high-speed networks:
	 * When measured RTT (m) is below 100 microseconds and current RTO is under 10ms,
	 * forcibly set RTO to half of RTO_MIN. This targets two scenarios:
	 *   - Loopback interfaces (localhost communication)
	 *   - Ultra-low-latency networks (e.g., InfiniBand, RDMA)
	 */
	if(m < 100 && conn->rttvar.rto < 10000)
	{
		conn->rttvar.rto = RTO_MIN / 2;
	}

	if (conn->rttvar.srtt != 0)
	{
		/* rtt = 7/8 rtt + 1/8 new */
		m -= (conn->rttvar.srtt >> LOSS_THRESH);
		conn->rttvar.srtt += m;
		if (m < 0)
		{
			m = -m;
			m -= (conn->rttvar.mdev >> RTT_SHIFT_ALPHA);
			if (m > 0)
				m >>= LOSS_THRESH;
		}
		else
		{
			m -= (conn->rttvar.mdev >> RTT_SHIFT_ALPHA);
		}
		conn->rttvar.mdev += m;
		if (conn->rttvar.mdev > conn->rttvar.mdev_max)
		{
			conn->rttvar.mdev_max = conn->rttvar.mdev;
			if (conn->rttvar.mdev_max > conn->rttvar.rttvar)
			{
				conn->rttvar.rttvar = conn->rttvar.mdev_max;
			}
		}
		if (UDP_SEQ_GT(conn->rttvar.snd_una, conn->rttvar.rtt_seq))
		{
			if (conn->rttvar.mdev_max < conn->rttvar.rttvar)
			{
				conn->rttvar.rttvar -= (conn->rttvar.rttvar - conn->rttvar.mdev_max) >> RTT_SHIFT_ALPHA;
			}
			conn->rttvar.mdev_max = rto_min;
		}
	}
	else
	{
		/* fresh measurement */
		conn->rttvar.srtt = m << LOSS_THRESH;
		conn->rttvar.mdev = m << 1;
		conn->rttvar.mdev_max = conn->rttvar.rttvar = MAX(conn->rttvar.mdev, rto_min);
	}

	conn->rttvar.rto = ((conn->rttvar.srtt >> LOSS_THRESH) + conn->rttvar.rttvar) > RTO_MAX ? RTO_MAX : ((conn->rttvar.srtt >> LOSS_THRESH) + conn->rttvar.rttvar);
}

/*
 * addCRC
 * 		add CRC field to the packet.
 */
static void
addCRC(icpkthdr *pkt)
{
	pkt->crc = ComputeCRC(pkt, pkt->len);
}

/*
 * checkCRC
 * 		check the validity of the packet.
 */
static bool
checkCRC(icpkthdr *pkt)
{
	uint32		rx_crc,
				local_crc;

	rx_crc = pkt->crc;
	pkt->crc = 0;
	local_crc = ComputeCRC(pkt, pkt->len);
	if (rx_crc != local_crc)
	{
		return false;
	}

	return true;
}


/*
 * checkRxThreadError
 * 		Check whether there was error in the background thread in main thread.
 *
 * 	If error found, report it.
 */
static void
checkRxThreadError()
{
	int eno;

	eno = ic_atomic_read_u32(&ic_control_info.eno);
	if (eno != 0)
	{
		errno = eno;

		std::stringstream ss;
		ss <<"ERROR, interconnect encountered an error, in receive background thread: "<<strerror(eno);
		throw ICReceiveThreadException(ss.str(), __FILE__, __LINE__);
	}
}

/*
 * setRxThreadError
 * 		Set the error no in background thread.
 *
 * 	Record the error in background thread. Main thread checks the errors periodically.
 * 	If main thread will find it, main thread will handle it.
 */
static void
setRxThreadError(int eno)
{
	uint32 expected = 0;

	/* always let main thread know the error that occurred first. */
	if (ic_atomic_compare_exchange_u32(&ic_control_info.eno, &expected, (uint32) eno))
	{
		LOG(LOG_ERROR, "Interconnect error: in background thread, set ic_control_info.eno to %d, "
					   "rx_buffer_pool.count %d, rx_buffer_pool.maxCount %d",
					   expected, rx_buffer_pool.count, rx_buffer_pool.maxCount);
	}
}

/*
 * resetRxThreadError
 * 		Reset the error no.
 *
 */
static void
resetRxThreadError()
{
	ic_atomic_write_u32(&ic_control_info.eno, 0);
}

/*
  * Set UDP IC send/receive socket buffer size.
  *
  * We must carefully size the UDP IC socket's send/receive buffers. If the size
  * is too small, say 128K, and send queue depth and receive queue depth are
  * large, then there might be a lot of dropped/reordered packets. We start
  * trying from a size of 2MB (unless Gp_udp_bufsize_k is specified), and
  * gradually back off to UDPIC_MIN_BUF_SIZE. For a given size setting to be
  * successful, the corresponding UDP kernel buffer size params must be adequate.
  *
 */
static uint32
setUDPSocketBufferSize(int ic_socket, int buffer_type)
{
    int                 expected_size;
    int                 curr_size;
    socklen_t           option_len = 0;

    Assert(buffer_type == SO_SNDBUF || buffer_type == SO_RCVBUF);

	expected_size = (global_param.Gp_udp_bufsize_k ? global_param.Gp_udp_bufsize_k * 1024 : 2048 * 1024);

    curr_size = expected_size;
    option_len = sizeof(curr_size);
    while (setsockopt(ic_socket, SOL_SOCKET, buffer_type, (const char *) &curr_size, option_len) < 0)
	{
		if(session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
			LOG(DEBUG3, "UDP-IC: setsockopt %s failed to set buffer size = %d bytes: %m",
                          buffer_type == SO_SNDBUF ? "send": "receive",
                          curr_size);

        curr_size = curr_size >> 1;
        if (curr_size < UDPIC_MIN_BUF_SIZE)
            return -1;
	}

	if(session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
    	LOG(DEBUG3, "UDP-IC: socket %s current buffer size = %d bytes",
                      buffer_type == SO_SNDBUF ? "send": "receive",
                      curr_size);

    return curr_size;
}


/*
 * setupUDPListeningSocket
 * 		Setup udp listening socket.
 */
static void
setupUDPListeningSocket(int *listenerSocketFd, int32 *listenerPort, int *txFamily, struct sockaddr_storage *listenerSockaddr)
{
    struct addrinfo         *addrs = NULL;
    struct addrinfo         *addr;
    struct addrinfo         hints;
    int                     ret;
    int                     ic_socket = INVALID_SOCKET;
    struct sockaddr_storage ic_socket_addr;
    int                     tries = 0;
    struct sockaddr_storage listenerAddr;
    socklen_t               listenerAddrlen = sizeof(ic_socket_addr);
    uint32                  socketSendBufferSize;
    uint32                  socketRecvBufferSize;

	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC;	/* Allow IPv4 or IPv6 */
	hints.ai_socktype = SOCK_DGRAM; /* Datagram socket */
	hints.ai_protocol = 0;
    hints.ai_addrlen = 0;
    hints.ai_addr = NULL;
    hints.ai_canonname = NULL;
    hints.ai_next = NULL;
    hints.ai_flags |= AI_NUMERICHOST;

#ifdef USE_ASSERT_CHECKING
	if (session_param.gp_udpic_network_disable_ipv6)
		hints.ai_family = AF_INET;
#endif

	if (global_param.Gp_interconnect_address_type == INTERCONNECT_ADDRESS_TYPE_UNICAST_IC)
	{
		Assert(global_param.interconnect_address && strlen(global_param.interconnect_address) > 0);
		hints.ai_flags |= AI_NUMERICHOST;
		if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
			LOG(DEBUG3, "getaddrinfo called with unicast address: %s", global_param.interconnect_address);
	}
	else
	{
		Assert(global_param.interconnect_address == NULL);
		hints.ai_flags |= AI_PASSIVE;
		if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
			LOG(DEBUG3, "getaddrinfo called with wildcard address");
	}

	/*
     * Restrict what IP address we will listen on to just the one that was
     * used to create this QE session.
	 */
    Assert(global_param.interconnect_address && strlen(global_param.interconnect_address) > 0);
    ret = getaddrinfo((!global_param.interconnect_address || global_param.interconnect_address[0] == '\0') ? NULL : global_param.interconnect_address,
			NULL, &hints, &addrs);
    if (ret || !addrs)
    {
        LOG(INFO, "could not resolve address for UDP IC socket %s: %s",
                        global_param.interconnect_address,
                        gai_strerror(ret));
        goto startup_failed;
    }

	/*
     * On some platforms, pg_getaddrinfo_all() may return multiple addresses
     * only one of which will actually work (eg, both IPv6 and IPv4 addresses
     * when kernel will reject IPv6).  Worse, the failure may occur at the
     * bind() or perhaps even connect() stage.  So we must loop through the
     * results till we find a working combination. We will generate DEBUG
     * messages, but no error, for bogus combinations.
	 */
	for (addr = addrs; addr != NULL; addr = addr->ai_next)
	{
#ifdef HAVE_UNIX_SOCKETS
        /* Ignore AF_UNIX sockets, if any are returned. */
        if (addr->ai_family == AF_UNIX)
            continue;
#endif

		if (++tries > 1 && session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
			LOG(DEBUG3, "trying another address for UDP interconnect socket");

        ic_socket = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
        if (ic_socket == INVALID_SOCKET)
		{
			if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
				LOG(DEBUG3, "could not create UDP interconnect socket: %m");
			continue;
		}

        /*
         * Bind the socket to a kernel assigned ephemeral port on the
         * interconnect_address.
         */
        if (bind(ic_socket, addr->ai_addr, addr->ai_addrlen) < 0)
		{
			if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
				LOG(DEBUG3, "could not bind UDP interconnect socket: %m");
            closesocket(ic_socket);
            ic_socket = INVALID_SOCKET;
            continue;
		}

        /* Call getsockname() to eventually obtain the assigned ephemeral port */
        if (getsockname(ic_socket, (struct sockaddr *) &listenerAddr, &listenerAddrlen) < 0)
		{
			if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
				LOG(DEBUG3, "could not get address of UDP interconnect socket: %m");
            closesocket(ic_socket);
            ic_socket = INVALID_SOCKET;
            continue;
		}

       /* If we get here, we have a working socket */
       break;
	}

    if (!addr || ic_socket == INVALID_SOCKET)
        goto startup_failed;

    /* Memorize the socket fd, kernel assigned port and address family */
    *listenerSocketFd = ic_socket;
    if (listenerAddr.ss_family == AF_INET6)
    {
        *listenerPort = ntohs(((struct sockaddr_in6 *) &listenerAddr)->sin6_port);
        *txFamily = AF_INET6;
    }
    else
    {
        *listenerPort = ntohs(((struct sockaddr_in *) &listenerAddr)->sin_port);
        *txFamily = AF_INET;
    }

    /*
     * cache the successful sockaddr of the listening socket, so
     * we can use this information to connect to the listening socket.
     */
    if (listenerSockaddr != NULL)
        memcpy(listenerSockaddr, &listenerAddr, sizeof(struct sockaddr_storage));

    /* Set up socket non-blocking mode */
    if (!ic_set_noblock(ic_socket))
    {
        LOG(INFO, "could not set UDP interconnect socket to nonblocking mode: %s", strerror(errno));
        goto startup_failed;
    }

    /* Set up the socket's send and receive buffer sizes. */
    socketRecvBufferSize = setUDPSocketBufferSize(ic_socket, SO_RCVBUF);
    if (socketRecvBufferSize == static_cast<uint32>(-1))
        goto startup_failed;
    ic_control_info.socketRecvBufferSize = socketRecvBufferSize;

    socketSendBufferSize = setUDPSocketBufferSize(ic_socket, SO_SNDBUF);
    if (socketSendBufferSize == static_cast<uint32>(-1))
        goto startup_failed;
    ic_control_info.socketSendBufferSize = socketSendBufferSize;

	if (addrs != NULL)
		freeaddrinfo(addrs);
	return;

startup_failed:
	if (addrs)
		freeaddrinfo(addrs);
	if (ic_socket != INVALID_SOCKET)
	{
		closesocket(ic_socket);
	}

	std::stringstream ss;
	ss << "ERROR,interconnect error: Could not set up udp listener socket: " << strerror(errno);
	throw ICNetworkException(ss.str(), __FILE__, __LINE__);
}

/*
 * InitMutex
 * 		Initialize mutex.
 */
static void
initMutex(pthread_mutex_t *mutex)
{
	pthread_mutexattr_t m_atts;

	pthread_mutexattr_init(&m_atts);
	pthread_mutexattr_settype(&m_atts, PTHREAD_MUTEX_ERRORCHECK);

	pthread_mutex_init(mutex, &m_atts);
}

/*
 * Set up the udp interconnect pthread signal mask, we don't want to run our signal handlers
 */
static void
ic_set_pthread_sigmasks(sigset_t *old_sigs)
{
#ifndef WIN32
	sigset_t sigs;
	int		 err;

	sigfillset(&sigs);

	err = pthread_sigmask(SIG_BLOCK, &sigs, old_sigs);
	if (err != 0)
	{
		std::stringstream ss;
		ss << "ERROR: Failed to get pthread signal masks with return value: "<<err;
		throw ICReceiveThreadException(ss.str(), __FILE__, __LINE__);
	}
#else
	(void) old_sigs;
#endif
}

static void
ic_reset_pthread_sigmasks(sigset_t *sigs)
{
#ifndef WIN32
	int err;

	err = pthread_sigmask(SIG_SETMASK, sigs, NULL);
	if (err != 0)
	{
		std::stringstream ss;
		ss <<"ERROR: Failed to reset pthread signal masks with return value: "<<err;
		throw ICReceiveThreadException(ss.str(), __FILE__, __LINE__);
	}
#else
	(void) sigs;
#endif
}

/*
 * InitMotionUDPIFC
 * 		Initialize UDP specific comms, and create rx-thread.
 */
static inline void
InitMotionUDPIFC(int *listenerSocketFd, int32 *listenerPort)
{
	int			pthread_err;
	int			txFamily = -1;

	/* attributes of the thread we're creating */
	pthread_attr_t t_atts;
	sigset_t	   pthread_sigs;

#ifdef USE_ASSERT_CHECKING
	set_test_mode();
#endif

	/* Initialize global ic control data. */
	ic_atomic_init_u32(&ic_control_info.eno, 0);
	ic_control_info.isSender = false;
	ic_control_info.socketSendBufferSize = 2 * 1024 * 1024;
	ic_control_info.socketRecvBufferSize = 2 * 1024 * 1024;
	initMutex(&ic_control_info.lock);
	ic_atomic_init_u32(&ic_control_info.shutdown, 0);
	ic_control_info.threadCreated = false;
	ic_control_info.ic_instance_id = 0;

	ic_control_info.connHtab.init();
	if (!ic_control_info.startupCacheHtab.init())
	{
		throw ICFatalException("FATAL, failed to initialize connection htab for startup cache", __FILE__, __LINE__);
	}

	/*
	 * setup listening socket and sending socket for Interconnect.
	 */
    setupUDPListeningSocket(listenerSocketFd, listenerPort, &txFamily, &udp_dummy_packet_sockaddr);
    setupUDPListeningSocket(&ICSenderSocket, &ICSenderPort, &ICSenderFamily, NULL);

	/* Initialize receive control data. */
	rx_control_info.mainWaitingState.reset();

	/* allocate a buffer for sending disorder messages */
	rx_control_info.disorderBuffer = (icpkthdr *)ic_malloc0(MIN_PACKET_SIZE);
	rx_control_info.lastDXatId = InvalidTransactionId;
	rx_control_info.lastTornIcId = 0;
	rx_control_info.cursorHistoryTable.init();

	/* Initialize receive buffer pool */
	rx_buffer_pool.count = 0;
	rx_buffer_pool.maxCount = 1;
	rx_buffer_pool.freeList = NULL;

	/* Initialize send control data */
	snd_control_info.cwnd = 0;
	snd_control_info.minCwnd = 0;
	snd_control_info.ackBuffer = (icpkthdr *)ic_malloc0(MIN_PACKET_SIZE);

	/* Start up our rx-thread */

	/*
	 * save ourselves some memory: the defaults for thread stack size are
	 * large (1M+)
	 */
	pthread_attr_init(&t_atts);

	pthread_attr_setstacksize(&t_atts, Max(PTHREAD_STACK_MIN, (128 * 1024)));
	ic_set_pthread_sigmasks(&pthread_sigs);
	pthread_err = pthread_create(&ic_control_info.threadHandle, &t_atts, rxThreadFunc, NULL);
	ic_reset_pthread_sigmasks(&pthread_sigs);

	pthread_attr_destroy(&t_atts);
	if (pthread_err != 0)
	{
		ic_control_info.threadCreated = false;
		std::stringstream ss;
		ss<<"FATAL: pthread_create() failed with err "<<pthread_err;
		throw ICFatalException(ss.str(), __FILE__, __LINE__);
	}

	ic_control_info.threadCreated = true;
}

/*
 * CleanupMotionUDPIFC
 * 		Clean up UDP specific stuff such as cursor ic hash table, thread etc.
 */
static inline void
CleanupMotionUDPIFC(void)
{
	LOG(DEBUG2, "udp-ic: telling receiver thread to shutdown.");

	/*
	 * We should not hold any lock when we reach here even when we report
	 * FATAL errors. Just in case, We still release the locks here.
	 */
	pthread_mutex_unlock(&ic_control_info.lock);

	/* Shutdown rx thread. */
	ic_atomic_write_u32(&ic_control_info.shutdown, 1);

	if (ic_control_info.threadCreated)
		pthread_join(ic_control_info.threadHandle, NULL);

	LOG(DEBUG2, "udp-ic: receiver thread shutdown.");

	rx_control_info.cursorHistoryTable.purge();

	ic_control_info.connHtab.destroy();

	/* background thread exited, we can do the cleanup without locking. */
	cleanupStartupCache();
	ic_control_info.startupCacheHtab.destroy();

	/* free the disorder buffer */
	ic_free(rx_control_info.disorderBuffer);
	rx_control_info.disorderBuffer = NULL;

	/* free the buffer for acks */
	ic_free(snd_control_info.ackBuffer);
	snd_control_info.ackBuffer = NULL;

	if (ICSenderSocket >= 0)
		closesocket(ICSenderSocket);
	ICSenderSocket = -1;
	ICSenderPort = 0;
	ICSenderFamily = 0;

    memset(&udp_dummy_packet_sockaddr, 0, sizeof(udp_dummy_packet_sockaddr));

#ifdef USE_ASSERT_CHECKING

	/*
	 * Check malloc times, in Interconnect part, memory are carefully released
	 * in tear down code (even when error occurred). But if a FATAL error is
	 * reported, tear down code will not be executed. Thus, it is still
	 * possible the malloc times and free times do not match when we reach
	 * here. The process will die in this case, the mismatch does not
	 * introduce issues.
	 */
	if (icudp_malloc_times != 0)
		LOG(INFO, "WARNING: malloc times and free times do not match. remain alloc times: %ld", icudp_malloc_times);
#endif
}

/*
 * getSockAddr
 * 		Convert IP addr and port to sockaddr
 */
static void
getSockAddr(struct sockaddr_storage *peer, socklen_t *peer_len, const char *listenerAddr, int listenerPort)
{
	int			ret;
	char		portNumberStr[32];
	char	   *service;
	struct addrinfo *addrs = NULL;
	struct addrinfo hint;

	/*
	 * Get socketaddr to connect to.
	 */

	/* Initialize hint structure */
	memset(&hint, 0, sizeof(hint));
	hint.ai_socktype = SOCK_DGRAM;	/* UDP */
	hint.ai_family = AF_UNSPEC; /* Allow for any family (v4, v6, even unix in
								 * the future)  */
#ifdef AI_NUMERICSERV
	hint.ai_flags = AI_NUMERICHOST | AI_NUMERICSERV;	/* Never do name
														 * resolution */
#else
	hint.ai_flags = AI_NUMERICHOST; /* Never do name resolution */
#endif

	snprintf(portNumberStr, sizeof(portNumberStr), "%d", listenerPort);
	service = portNumberStr;

	addrs = NULL;
	/* NULL has special meaning to getaddrinfo(). */
	ret = getaddrinfo((!listenerAddr || listenerAddr[0] == '\0') ? NULL : listenerAddr,
					 service, &hint, &addrs);
	if (ret || !addrs)
	{
		if (addrs)
			freeaddrinfo(addrs);

		std::stringstream ss;
		ss<<"ERROR, interconnect error: Could not parse remote listener address: '"<<listenerAddr<<
			"' port '"<<listenerPort<<"': "<<gai_strerror(ret)<<" getaddrinfo() unable to parse address: '"<<listenerAddr<<"'";
		throw ICNetworkException(ss.str(), __FILE__, __LINE__);
	}

	/*
	 * Since we aren't using name resolution, getaddrinfo will return only 1
	 * entry
	 */

	LOG(DEBUG1, "GetSockAddr socket ai_family %d ai_socktype %d ai_protocol %d for %s ",
				addrs->ai_family, addrs->ai_socktype, addrs->ai_protocol, listenerAddr);
	memset(peer, 0, sizeof(struct sockaddr_storage));
	memcpy(peer, addrs->ai_addr, addrs->ai_addrlen);
	*peer_len = addrs->ai_addrlen;

	if (addrs)
		freeaddrinfo(addrs);
}

/*
 * format_sockaddr
 *			Format a sockaddr to a human readable string
 *
 * This function must be kept threadsafe, elog/ereport/palloc etc are not
 * allowed within this function.
 */
static char *
format_sockaddr_udp(struct sockaddr_storage *sa, char *buf, size_t len)
{
	int			ret;
	char		remote_host[NI_MAXHOST];
	char		remote_port[NI_MAXSERV];

	ret = getnameinfo((const struct sockaddr *)sa, sizeof(struct sockaddr_storage),
							 remote_host, sizeof(remote_host),
							 remote_port, sizeof(remote_port),
							 NI_NUMERICHOST | NI_NUMERICSERV);
	if (ret != 0)
	{
		strncpy(remote_host, "???", sizeof(remote_host));
		strncpy(remote_port, "???", sizeof(remote_port));
	}

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

/*
 * setupOutgoingUDPConnection
 *		Setup outgoing UDP connection.
 */
static void
setupOutgoingUDPConnection(int icid, TransportEntry *pEntry, UDPConn *conn)
{
	ICCdbProcess *cdbProc = NULL;

	Assert(pEntry);

	cdbProc = conn->cdbProc;
	Assert(conn->state == mcsSetupOutgoingConnection);
	Assert(conn->cdbProc);

	conn->remoteContentId = cdbProc->contentid;
	conn->stat_min_ack_time = ~((uint64) 0);

	/* Save the information for the error message if getaddrinfo fails */
	if (strchr(cdbProc->listenerAddr, ':') != 0)
		snprintf(conn->remoteHostAndPort, sizeof(conn->remoteHostAndPort),
				 "[%s]:%d", cdbProc->listenerAddr, cdbProc->listenerPort);
	else
		snprintf(conn->remoteHostAndPort, sizeof(conn->remoteHostAndPort),
				 "%s:%d", cdbProc->listenerAddr, cdbProc->listenerPort);

	/*
	 * Get socketaddr to connect to.
	 */
	getSockAddr(&conn->peer, &conn->peer_len, cdbProc->listenerAddr, cdbProc->listenerPort);

	/* Save the destination IP address */
	format_sockaddr_udp(&conn->peer, conn->remoteHostAndPort,
				   sizeof(conn->remoteHostAndPort));

	Assert(conn->peer.ss_family == AF_INET || conn->peer.ss_family == AF_INET6);

	{
#ifdef USE_ASSERT_CHECKING
		{
			struct sockaddr_storage source_addr;
			socklen_t	source_addr_len;

			memset(&source_addr, 0, sizeof(source_addr));
			source_addr_len = sizeof(source_addr);

			if (getsockname(pEntry->txfd, (struct sockaddr *) &source_addr, &source_addr_len) == -1)
			{
				throw ICNetworkException(std::string("ERROR, interconnect Error: Could not get port from socket, %m")+strerror(errno), __FILE__, __LINE__);
			}
			Assert(pEntry->txfd_family == source_addr.ss_family);
		}
#endif

		/*
		 * If the socket was created with a different address family than the
		 * place we are sending to, we might need to do something special.
		 */
		if (pEntry->txfd_family != conn->peer.ss_family)
		{
			/*
			 * If the socket was created AF_INET6, but the address we want to
			 * send to is IPv4 (AF_INET), we might need to change the address
			 * format.  On Linux, it isn't necessary:  glibc automatically
			 * handles this.  But on MAC OSX and Solaris, we need to convert
			 * the IPv4 address to an V4-MAPPED address in AF_INET6 format.
			 */
			if (pEntry->txfd_family == AF_INET6)
			{
				LOG(DEBUG1, "We are inet6, remote is inet.  Converting to v4 mapped address.");
				ConvertToIPv4MappedAddr(&conn->peer, &conn->peer_len);
			}
			else
			{
				/*
				 * If we get here, something is really wrong.  We created the
				 * socket as IPv4-only (AF_INET), but the address we are
				 * trying to send to is IPv6.  It's possible we could have a
				 * V4-mapped address that we could convert to an IPv4 address,
				 * but there is currently no code path where that could
				 * happen.  So this must be an error.
				 */
				throw ICNetworkException("ERROR: Trying to use an IPv4 (AF_INET) socket to send to an IPv6 address", __FILE__, __LINE__);
			}
		}
	}

	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
		LOG(DEBUG1, "Interconnect connecting to seg%d slice%d %s pid=%d sockfd=%d",
					conn->remoteContentId, pEntry->recvSlice->sliceIndex, conn->remoteHostAndPort, conn->cdbProc->pid, conn->sockfd);

	/* send connection request */
	memset(&conn->conn_info, 0, sizeof(conn->conn_info));
	conn->conn_info.len = 0;
	conn->conn_info.flags = 0;
	conn->conn_info.motNodeId = pEntry->motNodeId;

	conn->conn_info.recvSliceIndex = pEntry->recvSlice->sliceIndex;
	conn->conn_info.sendSliceIndex = pEntry->sendSlice->sliceIndex;
	conn->conn_info.srcContentId = global_param.segindex;
	conn->conn_info.dstContentId = conn->cdbProc->contentid;

	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
		LOG(DEBUG1, "setupOutgoingUDPConnection: node %d route %d srccontent %d dstcontent %d: %s",
					pEntry->motNodeId, conn->route, global_param.segindex, conn->cdbProc->contentid, conn->remoteHostAndPort);

	conn->conn_info.srcListenerPort = UDP2_GetListenPortUDP();
	conn->conn_info.srcPid = global_param.MyProcPid;
	conn->conn_info.dstPid = conn->cdbProc->pid;
	conn->conn_info.dstListenerPort = conn->cdbProc->listenerPort;

	conn->conn_info.sessionId = session_param.gp_session_id;
	conn->conn_info.icId = icid;

	ic_control_info.connHtab.add(conn);

	/*
	 * No need to get the connection lock here, since background rx thread
	 * will never access send connections.
	 */
	conn->msgPos = NULL;
	conn->msgSize = sizeof(conn->conn_info);
	conn->stillActive = true;
	conn->conn_info.seq = 1;
	conn->rttvar.ts_rto = 0;
	conn->rttvar.rto = UDP_INITIAL_RTO;
	conn->rttvar.srtt = 0;
	conn->rttvar.rttvar = 0;
	conn->rttvar.snd_una = 0;
	conn->rttvar.nrtx = 0;
	conn->rttvar.max_nrtx = 0;
	conn->rttvar.mss = UDP_DEFAULT_MSS;
	conn->rttvar.cwnd = 2;
	conn->rttvar.ssthresh = UDP_INFINITE_SSTHRESH;
	conn->rttvar.loss_count = 0;
	conn->rttvar.karn_mode = false;
	conn->on_rto_idx = -1;
	Assert(conn->peer.ss_family == AF_INET || conn->peer.ss_family == AF_INET6);
}

/*
 * If the socket was created AF_INET6, but the address we want to
 * send to is IPv4 (AF_INET), we need to change the address
 * format. On Linux, this is not necessary: glibc automatically
 * handles this. But on MAC OSX and Solaris, we need to convert
 * the IPv4 address to IPv4-mapped IPv6 address in AF_INET6 format.
 *
 * The comment above relies on getaddrinfo() via function getSockAddr to get
 * the correct V4-mapped address. We need to be careful here as we need to
 * ensure that the platform we are using is POSIX 1003-2001 compliant.
 * Just to be on the safeside, we'll be keeping this function for
 * now to be used for all platforms and not rely on POSIX.
 *
 * Since this can be called in a signal handler, we avoid the use of
 * async-signal unsafe functions such as memset/memcpy
 */
static void
ConvertToIPv4MappedAddr(struct sockaddr_storage *sockaddr, socklen_t *o_len)
{
	const struct sockaddr_in *in = (const struct sockaddr_in *)sockaddr;
	struct sockaddr_storage temp = {0};
	struct sockaddr_in6 *in6_new = (struct sockaddr_in6 *)&temp;

	/* Construct a IPv4-to-IPv6 mapped address.  */
	temp.ss_family = AF_INET6;
	in6_new->sin6_family = AF_INET6;
	in6_new->sin6_port = in->sin_port;
	in6_new->sin6_flowinfo = 0;

	((uint16 *)&in6_new->sin6_addr)[5] = 0xffff;

	in6_new->sin6_addr.s6_addr32[3] = in->sin_addr.s_addr;
	in6_new->sin6_scope_id = 0;

	/* copy it back */
	*sockaddr = temp;
	*o_len = sizeof(struct sockaddr_in6);
}

#if defined(__darwin__)
/* macos does not accept :: as the destination, we will need to covert this to the IPv6 loopback */
static void
ConvertIPv6WildcardToLoopback(struct sockaddr_storage *dest)
{
	char address[INET6_ADDRSTRLEN];
	/* we want to terminate our own process, so this should be local */
	const struct sockaddr_in6 *in6 = (const struct sockaddr_in6 *)&udp_dummy_packet_sockaddr;
	inet_ntop(AF_INET6, &in6->sin6_addr, address, sizeof(address));
	if (strcmp("::", address) == 0)
		((struct sockaddr_in6 *)dest)->sin6_addr = in6addr_loopback;
}
#endif

/*
 * handleCachedPackets
 * 		Deal with cached packets.
 */
static void
handleCachedPackets(void)
{
	UDPConn *cachedConn = NULL;
	UDPConn *setupConn = NULL;
	ConnHtabBin *bin = NULL;
	icpkthdr   *pkt = NULL;
	AckSendParam param;
	int			i = 0;
	uint32		j = 0;
	bool		dummy;

	for (i = 0; i < ic_control_info.startupCacheHtab.size; i++)
	{
		bin = ic_control_info.startupCacheHtab.table[i];

		while (bin)
		{
			cachedConn = bin->conn;
			setupConn = NULL;

			for (j = 0; j < cachedConn->pkt_q_size; j++)
			{
				pkt = (icpkthdr *) cachedConn->pkt_q[j];

				if (pkt == NULL)
					continue;

				rx_buffer_pool.maxCount--;

				/* look up this pkt's connection in connHtab */
				setupConn = ic_control_info.connHtab.find(pkt);
				if (setupConn == NULL)
				{
					/* mismatch! */
					rx_buffer_pool.put(pkt);
					cachedConn->pkt_q[j] = NULL;
					continue;
				}

				memset(&param, 0, sizeof(param));
				if (!handleDataPacket(setupConn, pkt, &cachedConn->peer, &cachedConn->peer_len, &param, &dummy))
				{
					/* no need to cache this packet */
					rx_buffer_pool.put(pkt);
				}

				ic_statistics.recvPktNum++;
				if (param.msg.len != 0)
					UDPConn::sendAckWithParam(&param);

				cachedConn->pkt_q[j] = NULL;
			}
			bin = bin->next;
			ic_control_info.startupCacheHtab.remove(cachedConn);

			/*
			 * MPP-19981 free the cached connections; otherwise memory leak
			 * would be introduced.
			 */
			ic_free(cachedConn->pkt_q);
			delete cachedConn;
		}
	}
}

/*
 * CChunkTransportStateImpl::setup
 * 		Internal function for setting up UDP interconnect.
 */
ICChunkTransportState*
CChunkTransportStateImpl::setup(ICSliceTable *sliceTable)
{
	pthread_mutex_lock(&ic_control_info.lock);

	Assert(sliceTable->ic_instance_id > 0);

	if (global_param.Gp_role == GP_ROLE_DISPATCH_IC)
	{
		/*
		 * QD use cursorHistoryTable to handle mismatch packets, no
		 * need to update ic_control_info.ic_instance_id
		 */
		Assert(session_param.gp_interconnect_id == sliceTable->ic_instance_id);
	}
	else
	{
		/*
		 * update ic_control_info.ic_instance_id, it is mainly used
		 * by rx thread to handle mismatch packets
		 */
		ic_control_info.ic_instance_id = sliceTable->ic_instance_id;
	}

	CChunkTransportStateImpl *state_impl = new CChunkTransportStateImpl(sliceTable);
	ICChunkTransportState *interconnect_context = static_cast<ICChunkTransportState*>(state_impl);
	CChunkTransportStateImpl::state_ = static_cast<CChunkTransportState*>(state_impl);

#ifdef USE_ASSERT_CHECKING
	ICExecSlice *mySlice = &interconnect_context->sliceTable->slices[sliceTable->localSlice];
	Assert(mySlice && mySlice->sliceIndex == sliceTable->localSlice);
#endif

#ifdef USE_ASSERT_CHECKING
	set_test_mode();
#endif

	if (global_param.Gp_role == GP_ROLE_DISPATCH_IC)
	{
		CursorICHistoryTable *ich_table = &rx_control_info.cursorHistoryTable;
		//DistributedTransactionId distTransId = getDistributedTransactionId(); TODO: add callback;
		DistributedTransactionId distTransId = InvalidTransactionId;

       if (ich_table->count > (2 * ich_table->size))
       {
           /*
            * distTransId != lastDXatId
            * Means the last transaction is finished, it's ok to make a prune.
            */
            if (distTransId != rx_control_info.lastDXatId)
            {
                if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
                    LOG(DEBUG1, "prune cursor history table (count %d), icid %d, prune_id %d",
                        ich_table->count, sliceTable->ic_instance_id, sliceTable->ic_instance_id);
			   ich_table->prune(sliceTable->ic_instance_id);
            }
            /*
             * distTransId == lastDXatId and they are not InvalidTransactionId(0)
             * Means current (non Read-Only) transaction isn't finished, should not prune.
             */
            else if (rx_control_info.lastDXatId != InvalidTransactionId)
            {
                ;
            }
            /*
             * distTransId == lastDXatId and they are InvalidTransactionId(0)
             * Means they are the same transaction or different Read-Only transactions.
             *
             * For the latter, it's hard to get a perfect timepoint to prune: prune eagerly may
             * cause problems (pruned current Txn's Ic instances), but prune in low frequency
             * causes memory leak.
             *
             * So, we choose a simple algorithm to prune it here. And if it mistakenly prune out
             * the still-in-used Ic instance (with lower id), the query may hang forever.
             * Then user have to set a bigger gp_interconnect_cursor_ic_table_size value and
             * try the query again, it is a workaround.
             *
             * More backgrounds please see: https://github.com/greenplum-db/gpdb/pull/16458
             */
            else
            {
                if (sliceTable->ic_instance_id > ich_table->size)
                {
                    uint32 prune_id = sliceTable->ic_instance_id - ich_table->size;
                    Assert(prune_id < sliceTable->ic_instance_id);
 
                    if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
                        LOG(DEBUG1, "prune cursor history table (count %d), icid %d, prune_id %d",
                            ich_table->count, sliceTable->ic_instance_id, prune_id);
                    ich_table->prune(prune_id);
               }
			}
		}

		ich_table->add(sliceTable->ic_instance_id, session_param.gp_command_count);
		/* save the latest transaction id */
		rx_control_info.lastDXatId = distTransId;
	}

	/* Initiate receiving connections. */
	state_impl->CreateRecvEntries(sliceTable);

	/* Initiate outgoing connections. */
	state_impl->CreateSendEntries(sliceTable);

	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
		LOG(DEBUG1, "SetupUDPInterconnect will activate Listening on ports=%d/%d sockfd=%d.", 0, UDP2_GetListenPortUDP(), UDP_listenerFd);

	/*
	 * If there are packets cached by background thread, add them to the
	 * connections.
	 */
	if (session_param.gp_interconnect_cache_future_packets)
		handleCachedPackets();

	interconnect_context->activated = true;

	pthread_mutex_unlock(&ic_control_info.lock);

	return interconnect_context;
}

/*
 * sendControlMessage
 * 		Helper function to send a control message.
 */
void
UDPConn::sendControlMessage(icpkthdr *pkt, int fd, struct sockaddr *addr, socklen_t peerLen)
{
	int			n;

#ifdef USE_ASSERT_CHECKING
	if (testmode_inject_fault(session_param.gp_udpic_dropacks_percent))
	{
#ifdef AMS_VERBOSE_LOGGING
		LOG(INFO, "THROW CONTROL MESSAGE with seq %d extraSeq %d srcpid %d despid %d", pkt->seq, pkt->extraSeq, pkt->srcPid, pkt->dstPid);
#endif
		return;
	}
#endif

	/* Add CRC for the control message. */
	if (session_param.gp_interconnect_full_crc)
		addCRC(pkt);

	/* retry 10 times for sending control message */
	int counter = 0;
	while (counter < 10)
	{
		counter++;
		n = sendto(fd, (const char *)pkt, pkt->len, 0, addr, peerLen);
		if (n < 0)
		{
			if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
				continue;
			else
			{
				LOG(INFO, "sendcontrolmessage: got errno %d", errno);
				return;
			}
		}
		break;
	}
	if (n < int(pkt->len))
		LOG(INFO, "sendcontrolmessage: got error %d errno %d seq %d", n, errno, pkt->seq);
}

/*
 * setAckParam
 * 		Set the ack sending parameters.
 */
void
UDPConn::setAckParam(AckSendParam *param, int32 flags, uint32 seq, uint32 extraSeq)
{
	memcpy(&param->msg, (char *) &this->conn_info, sizeof(icpkthdr));
	param->msg.flags = flags;
	param->msg.seq   = seq;
	param->msg.extraSeq = extraSeq;
	param->msg.len   = sizeof(icpkthdr);
	param->peer      = this->peer;
	param->peer_len  = this->peer_len;
}

/*
 * sendAckWithParam
 * 		Send acknowledgment to sender.
 */
void
UDPConn::sendAckWithParam(AckSendParam *param)
{
	sendControlMessage(&param->msg, UDP_listenerFd, (struct sockaddr *) &param->peer, param->peer_len);
}

/*
 * sendAck
 * 		Send acknowledgment to sender.
 */
void
UDPConn::sendAck(int32 flags, uint32 seq, uint32 extraSeq)
{
	icpkthdr	msg;

	memcpy(&msg, (char *) &this->conn_info, sizeof(msg));
	msg.flags = flags;
	msg.seq   = seq;
	msg.extraSeq = extraSeq;
	msg.len = sizeof(icpkthdr);

	LOG(DEBUG1, "sendack: node %d route %d seq %d extraSeq %d, flags %s", msg.motNodeId, this->route, msg.seq, msg.extraSeq, flags2txt(msg.flags));

	sendControlMessage(&msg, UDP_listenerFd, (struct sockaddr *) &this->peer, this->peer_len);
}

/*
 * sendDisorderAck
 *		Send a disorder message to the sender.
 *
 * Whenever the receiver detects a disorder packet, it will assemble a disorder message
 * which contains the sequence numbers of the possibly lost packets.
 *
 */
void
UDPConn::sendDisorderAck(uint32 seq, uint32 extraSeq, uint32 lostPktCnt)
{
	icpkthdr   *disorderBuffer = rx_control_info.disorderBuffer;

	memcpy(disorderBuffer, (char *) &this->conn_info, sizeof(icpkthdr));

	disorderBuffer->flags |= UDPIC_FLAGS_DISORDER;
	disorderBuffer->seq = seq;
	disorderBuffer->extraSeq = extraSeq;
	disorderBuffer->len = lostPktCnt * sizeof(uint32) + sizeof(icpkthdr);

#ifdef AMS_VERBOSE_LOGGING
	if (!(this->peer.ss_family == AF_INET || this->peer.ss_family == AF_INET6))
	{
		LOG(INFO, "UDP Interconnect bug (in sendDisorderAck): trying to send ack when we don't know where to send to %s", this->remoteHostAndPort);
	}
#endif

	sendControlMessage(disorderBuffer, UDP_listenerFd, (struct sockaddr *) &this->peer, this->peer_len);
}

/*
 * sendStatusQueryMessage
 *		Used by senders to send a status query message for a connection to receivers.
 *
 * When receivers get such a message, they will respond with
 * the connection status (consumed seq, received seq ...).
 */
void
UDPConn::sendStatusQueryMessage(uint32 seq)
{
	icpkthdr	msg;

	memcpy(&msg, (char *) &this->conn_info, sizeof(msg));
	msg.flags = UDPIC_FLAGS_CAPACITY;
	msg.seq = seq;
	msg.extraSeq = 0;
	msg.len = sizeof(msg);

#ifdef TRANSFER_PROTOCOL_STATS
	trans_proto_stats.update(TPE_ACK_PKT_QUERY, &msg);
#endif

	sendControlMessage(&msg, entry_->txfd, (struct sockaddr *) &this->peer, this->peer_len);
}

/*
 * ReleaseBuffer
 * 		Return a buffer and send an acknowledgment.
 *
 *  SHOULD BE CALLED WITH ic_control_info.lock *LOCKED*
 */
void
UDPConn::ReleaseBuffer(AckSendParam *param)
{
	icpkthdr   *buf;
	uint32		seq;

	buf = (icpkthdr *) this->pkt_q[this->pkt_q_head];
	if (buf == NULL)
	{
		pthread_mutex_unlock(&ic_control_info.lock);
		throw ICFatalException("FATAL: ReleaseBuffer: buffer is NULL", __FILE__, __LINE__);
	}

	seq = buf->seq;

#ifdef AMS_VERBOSE_LOGGING
	LOG(INFO, "LOG: ReleaseBuffer conn %p pkt [seq %d] for node %d route %d, [head seq] %d queue size %d, queue head %d queue tail %d",
			  this, seq, buf->motNodeId, this->route, this->conn_info.seq - this->pkt_q_size, this->pkt_q_size, this->pkt_q_head, this->pkt_q_tail);
#endif

	this->pkt_q[this->pkt_q_head] = NULL;
	this->pBuff = NULL;
	this->pkt_q_head = (this->pkt_q_head + 1) % this->pkt_q_capacity;
	this->pkt_q_size--;

#ifdef AMS_VERBOSE_LOGGING
	LOG(INFO, "LOG: ReleaseBuffer conn %p pkt [seq %d] for node %d route %d, [head seq] %d queue size %d, queue head %d queue tail %d",
			  this, seq, buf->motNodeId, this->route, this->conn_info.seq - this->pkt_q_size, this->pkt_q_size, this->pkt_q_head, this->pkt_q_tail);
#endif

	rx_buffer_pool.put(buf);
	this->conn_info.extraSeq = seq;

	/* Send an Ack to the sender. */
	if ((seq % 2 == 0) || (this->pkt_q_capacity == 1))
	{
		if (param != NULL)
		{
			this->setAckParam(param, UDPIC_FLAGS_ACK | UDPIC_FLAGS_CAPACITY | this->conn_info.flags, this->conn_info.seq - 1, seq);
		}
		else
		{
			this->sendAck(UDPIC_FLAGS_ACK | UDPIC_FLAGS_CAPACITY | this->conn_info.flags, this->conn_info.seq - 1, seq);
		}
	}
}

/*
 * computeExpirationPeriod
 * 		Compute expiration period according to the connection information.
 *
 * Considerations on expiration period computation:
 *
 * RTT is dynamically computed, and expiration period is based on RTT values.
 * We cannot simply use RTT as the expiration value, since real workload does
 * not always have a stable RTT. A small constant value is multiplied to the RTT value
 * to make the resending logic insensitive to the frequent small changes of RTT.
 *
 */
uint64
UDPConn::computeExpirationPeriod(uint32 retry)
{
	/*
	 * In fault injection mode, we often use DEFAULT_RTT, because the
	 * intentional large percent of packet/ack losses will make the RTT too
	 * large. This will lead to a slow retransmit speed. In real hardware
	 * environment/workload, we do not expect such a packet loss pattern.
	 */
#ifdef USE_ASSERT_CHECKING
	if (udp_testmode)
	{
		return DEFAULT_RTT;
	}
	else
#endif
	{
		if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_ADVANCE_IC)
			return Min(retry > 3 ? this->rttvar.rto * retry : this->rttvar.rto, UNACK_QUEUE_RING_LENGTH_LOSS);

		uint32 factor = (retry <= 12 ? retry : 12);
		return Max(MIN_EXPIRATION_PERIOD, Min(MAX_EXPIRATION_PERIOD, (int)(this->rtt + (this->dev << 2)) << (factor)));
	}
}

/*
 * freeDisorderedPackets
 * 		Put the disordered packets into free buffer list.
 */
void
UDPConn::freeDisorderedPackets()
{
	uint32		k;

	if (this->pkt_q == NULL)
		return;

	for (k = 0; k < this->pkt_q_capacity; k++)
	{
		icpkthdr   *buf = (icpkthdr *)this->pkt_q[k];

		if (buf != NULL)
		{
			if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
				LOG(DEBUG1, "CLEAR Out-of-order PKT: conn %p pkt [seq %d] for node %d route %d, "
							"[head seq] %d queue size %d, queue head %d queue tail %d",
							this, buf->seq, buf->motNodeId, this->route, this->conn_info.seq - this->pkt_q_size,
							this->pkt_q_size, this->pkt_q_head, this->pkt_q_tail);

			/* return the buffer into the free list. */
			rx_buffer_pool.put(buf);
			this->pkt_q[k] = NULL;
		}
	}
}

/*
 * prepareRxConnForRead
 * 		Prepare the receive connection for reading.
 *
 * MUST BE CALLED WITH ic_control_info.lock LOCKED.
 */
void
UDPConn::prepareRxConnForRead()
{
	LOG(DEBUG3, "In prepareRxConnForRead: conn %p, q_head %d q_tail %d q_size %d",
			this, this->pkt_q_head, this->pkt_q_tail, this->pkt_q_size);

	Assert(this->pkt_q[this->pkt_q_head] != NULL);
	this->pBuff     = this->pkt_q[this->pkt_q_head];
	this->msgPos    = this->pBuff;
	this->msgSize   = ((icpkthdr *) this->pBuff)->len;
	this->recvBytes = this->msgSize;
}

/*
 * DeactiveConn
 * 		Mark the connection inactive.
 */
void
UDPConn::DeactiveConn()
{
	pthread_mutex_lock(&ic_control_info.lock);
	this->stillActive = false;
	pthread_mutex_unlock(&ic_control_info.lock);
}

/*
 * handleAckedPacket
 * 		Called by sender to process acked packet.
 *
 * 	Remove it from unack queue and unack queue ring, change the rtt ...
 *
 * 	RTT (Round Trip Time) is computed as the time between we send the packet
 * 	and receive the acknowledgement for the packet. When an acknowledgement
 * 	is received, an estimated RTT value (called SRTT, smoothed RTT) is updated
 * 	by using the following equation. And we also set a limitation of the max
 * 	value and min value for SRTT.
 *	    (1) SRTT = (1 - g) SRTT + g x RTT (0 < g < 1)
 *	where RTT is the measured round trip time of the packet. In implementation,
 *	g is set to 1/8. In order to compute expiration period, we also compute an
 *	estimated delay variance SDEV by using:
 *	    (2) SDEV = (1 - h) x SDEV + h x |SERR| (0 < h < 1, In implementation, h is set to 1/4)
 *	where SERR is calculated by using:
 *	    (3) SERR = RTT - SRTT
 *	Expiration period determines the timing we resend a packet. A long RTT means
 *	a long expiration period. Delay variance is used to incorporate the variance
 *	of workload/network variances at different time. When a packet is retransmitted,
 *	we back off exponentially the expiration period.
 *	    (4) exp_period = (SRTT + y x SDEV) << retry
 *	Here y is a constant (In implementation, we use 4) and retry is the times the
 *	packet is retransmitted.
 */
void
UDPConn::handleAckedPacket(ICBuffer *buf, uint64 now, struct icpkthdr *pkt)
{
	uint64		ackTime = 0;
	bool		bufIsHead = false; 
	UDPConn *bufConn = NULL;

	bufIsHead = (&buf->primary == this->unackQueue.first());

	buf = this->unackQueue.remove(buf);

	if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_ADVANCE_IC || session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_TIMER_IC)
	{
		bufConn = static_cast<UDPConn*>(buf->conn);
		buf = unack_queue_ring.slots[buf->unackQueueRingSlot].remove(buf);
		unack_queue_ring.numOutStanding--;
		if (this->unackQueue.length() >= 1)
		unack_queue_ring.numSharedOutStanding--;

		ackTime = now - buf->sentTime;

		if (buf->nRetry == 0)
		{
			/* adjust the congestion control window. */
			if (snd_control_info.cwnd < snd_control_info.ssthresh)
				snd_control_info.cwnd += 2;
			else
				snd_control_info.cwnd += 1 / snd_control_info.cwnd;
			snd_control_info.cwnd = Min(snd_control_info.cwnd, snd_buffer_pool.maxCount);
		}

		if ((bufConn->rttvar.rto << 1) > ackTime && pkt->retry_times != session_param.Gp_interconnect_min_retries_before_timeout)
			estimateRTT(bufConn, (now - pkt->send_time));

		if (buf->nRetry && pkt->retry_times > 0 && pkt->retry_times < session_param.Gp_interconnect_min_retries_before_timeout)
			bufConn->rttvar.rto += (bufConn->rttvar.rto >> 4 * buf->nRetry);

		if (unlikely(session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_TIMER_IC))
		{
			bufConn->sndvar.ts_rto = bufConn->rttvar.rto;
			addtoRTOList(&mudp, bufConn);
		}
	}

	if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_IC)
	{
		ICBufferList *alist = &unack_queue_ring.slots[buf->unackQueueRingSlot];
		buf = alist->remove(buf);
		unack_queue_ring.numOutStanding--;
		if (this->unackQueue.length() >= 1)
			unack_queue_ring.numSharedOutStanding--;

		ackTime = now - buf->sentTime;

		/*
		 * In udp_testmode, we do not change rtt dynamically due to the large
		 * number of packet losses introduced by fault injection code. This
		 * can decrease the testing time.
		 */
#ifdef USE_ASSERT_CHECKING
		if (!udp_testmode)
#endif
		{
			uint64		newRTT = 0;
			uint64		newDEV = 0;

			if (buf->nRetry == 0)
			{
				bufConn = static_cast<UDPConn*>(buf->conn);
				newRTT = bufConn->rtt - (bufConn->rtt >> RTT_SHIFT_COEFFICIENT) + (ackTime >> RTT_SHIFT_COEFFICIENT);
				newRTT = Min(MAX_RTT, Max(newRTT, MIN_RTT));
				bufConn->rtt = newRTT;

				newDEV = bufConn->dev - (bufConn->dev >> DEV_SHIFT_COEFFICIENT) + ((Max(ackTime, newRTT) - Min(ackTime, newRTT)) >> DEV_SHIFT_COEFFICIENT);
				newDEV = Min(MAX_DEV, Max(newDEV, MIN_DEV));
				bufConn->dev = newDEV;

				/* adjust the congestion control window. */
				if (snd_control_info.cwnd < snd_control_info.ssthresh)
					snd_control_info.cwnd += 1;
				else
					snd_control_info.cwnd += 1 / snd_control_info.cwnd;
				snd_control_info.cwnd = Min(snd_control_info.cwnd, snd_buffer_pool.maxCount);
			}
		}
	}

	bufConn = static_cast<UDPConn*>(buf->conn);
	bufConn->stat_total_ack_time += ackTime;
	bufConn->stat_max_ack_time = Max(ackTime, bufConn->stat_max_ack_time);
	bufConn->stat_min_ack_time = Min(ackTime, bufConn->stat_min_ack_time);

	/*
	 * only change receivedAckSeq when it is the smallest pkt we sent and have
	 * not received ack for it.
	 */
	if (bufIsHead)
		this->receivedAckSeq = buf->pkt->seq;

	/* The first packet acts like a connect setup packet */
	if (buf->pkt->seq == 1)
		this->state = mcsStarted;

	snd_buffer_pool.freeList.append(buf);

#ifdef AMS_VERBOSE_LOGGING
	LOG(INFO, "REMOVEPKT %d from unack queue for route %d (retry %d) sndbufmaxcount %d sndbufcount %d "
			  "sndbuffreelistlen %d, sntSeq %d consumedSeq %d recvAckSeq %d capacity %d, sndQ %d, unackQ %d",
			  buf->pkt->seq, this->route, buf->nRetry, snd_buffer_pool.maxCount, snd_buffer_pool.count,
			  snd_buffer_pool.freeList.length(), bufConn->sentSeq, bufConn->consumedSeq,
			  bufConn->receivedAckSeq, bufConn->capacity, bufConn->sndQueue.length(),
			  bufConn->unackQueue.length());
	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
	{
		bufConn->unackQueue.icBufferListLog();
		bufConn->sndQueue.icBufferListLog();
	}
#endif
}

/*
 * dispatcherAYT
 * 		Check the connection from the dispatcher to verify that it is still there.
 *
 * The connection is a struct Port, stored in the global MyProcPort.
 *
 * Return true if the dispatcher connection is still alive.
 */
static bool
dispatcherAYT(void)
{
	ssize_t		ret;
	char		buf;

	/*
	 * For background worker or auxiliary process like gdd, there is no client.
	 * As a result, MyProcPort is NULL. We should skip dispatcherAYT check here.
	 */
	if (global_param.MyProcPort == false)
		return true;

	if (global_param.myprocport_sock < 0)
		return false;

#ifndef WIN32
	ret = recv(global_param.myprocport_sock, &buf, 1, MSG_PEEK | MSG_DONTWAIT);
#else
	ret = recv(global_param.myprocport_sock, &buf, 1, MSG_PEEK | MSG_PARTIAL);
#endif

	if (ret == 0)				/* socket has been closed. EOF */
		return false;

	if (ret > 0)				/* data waiting on socket, it must be OK. */
		return true;

	if (ret == -1)				/* error, or would be block. */
	{
		if (errno == EAGAIN || errno == EINPROGRESS)
			return true;		/* connection intact, no data available */
		else
			return false;
	}
	/* not reached */

	return true;
}

/*
 * checkQDConnectionAlive
 * 		Check whether QD connection is still alive. If not, report error.
 */
static void
checkQDConnectionAlive(void)
{
	if (!dispatcherAYT())
	{
		if (global_param.Gp_role == GP_ROLE_EXECUTE_IC)
			throw ICNetworkException("interconnect error segment lost contact with master (recv)", __FILE__, __LINE__);
		else
			throw ICNetworkException("interconnect error master lost contact with client (recv)", __FILE__, __LINE__);
	}
}

/*
 * getCurrentTime
 * 		get current time
 *
 */
static uint64
getCurrentTime(void)
{
	struct timeval newTime;
	int			status = 1;
	uint64		t = 0;

#if HAVE_LIBRT
	/* Use clock_gettime to return monotonic time value. */
	struct timespec ts;

	status = clock_gettime(CLOCK_MONOTONIC, &ts);

	newTime.tv_sec = ts.tv_sec;
	newTime.tv_usec = ts.tv_nsec / 1000;

#endif

	if (status != 0)
		gettimeofday(&newTime, NULL);

	t = ((uint64) newTime.tv_sec) * USECS_PER_SECOND + newTime.tv_usec;
	return t;
}

/*
 * putIntoUnackQueueRing
 * 		Put the buffer into the ring.
 *
 * expTime - expiration time from now
 *
 */
static void
putIntoUnackQueueRing(UnackQueueRing *uqr, ICBuffer *buf, uint64 expTime, uint64 now)
{
	UDPConn *buffConn = static_cast<UDPConn*>(buf->conn);
	uint64		diff = 0;
	int			idx = 0;
	
	if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_ADVANCE_IC)
	{
		/* The first packet, currentTime is not initialized */
#ifndef TIMEOUT_Z
		if (uqr->currentTime == 0)
			uqr->currentTime = now - (now % TIMER_SPAN_LOSS);
#else
		if (uqr->currentTime == 0 && buffConn->rttvar.rto == 0)
			uqr->currentTime = now - (now % TIMER_SPAN_LOSS);
		else
			uqr->currentTime = now + buffConn->rttvar.rto;

#endif
		diff = expTime;
		if (diff >= UNACK_QUEUE_RING_LENGTH_LOSS)
		{
#ifdef AMS_VERBOSE_LOGGING
			LOG(INFO, "putIntoUnackQueueRing:" "now " UINT64_FORMAT "expTime " UINT64_FORMAT "diff " UINT64_FORMAT "uqr-currentTime " UINT64_FORMAT, now, expTime, diff, uqr->currentTime);
#endif
			diff = UNACK_QUEUE_RING_LENGTH_LOSS - 1;
		}
		else if (diff < TIMER_SPAN_LOSS)
		{
			diff = diff < TIMER_SPAN_LOSS ? TIMER_SPAN_LOSS : diff;
		}
	}
	else
	{
		if (uqr->currentTime == 0)
		uqr->currentTime = now - (now % TIMER_SPAN_LOSS);

		diff = now + expTime - uqr->currentTime;
		if (diff >= UNACK_QUEUE_RING_LENGTH)
		{
#ifdef AMS_VERBOSE_LOGGING
			LOG(INFO, "putIntoUnackQueueRing:" "now " UINT64_FORMAT "expTime " UINT64_FORMAT "diff " UINT64_FORMAT "uqr-currentTime " UINT64_FORMAT, now, expTime, diff, uqr->currentTime);
#endif
			diff = UNACK_QUEUE_RING_LENGTH - 1;
		}
		else if (diff < TIMER_SPAN)
		{
			diff = TIMER_SPAN;
		}

		idx = (uqr->idx + diff / TIMER_SPAN) % UNACK_QUEUE_RING_SLOTS_NUM;

#ifdef AMS_VERBOSE_LOGGING
		LOG(INFO, "PUTTW: curtime " UINT64_FORMAT " now " UINT64_FORMAT " (diff " UINT64_FORMAT ") expTime " UINT64_FORMAT " previdx %d, nowidx %d, nextidx %d", uqr->currentTime, now, diff, expTime, buf->unackQueueRingSlot, uqr->idx, idx);
#endif
	}

	idx = (uqr->idx + diff / TIMER_SPAN) % UNACK_QUEUE_RING_SLOTS_NUM;

#ifdef AMS_VERBOSE_LOGGING
	LOG(INFO, "PUTTW: curtime %lu now %lu (diff %lu) expTime %lu previdx %d, nowidx %d, nextidx %d", uqr->currentTime, now, diff, expTime, buf->unackQueueRingSlot, uqr->idx, idx);
#endif

	buf->unackQueueRingSlot = idx;
	unack_queue_ring.slots[idx].append(buf);
}

/*
 * handleDataPacket
 * 		Handling the data packet.
 *
 * On return, will set *wakeup_mainthread, if a packet was received successfully
 * and the caller should wake up the main thread, after releasing the mutex.
 */
static bool
handleDataPacket(UDPConn *conn, icpkthdr *pkt, struct sockaddr_storage *peer, socklen_t *peerlen,
				 AckSendParam *param, bool *wakeup_mainthread)
{
	if ((pkt->len == sizeof(icpkthdr)) && (pkt->flags & UDPIC_FLAGS_CAPACITY))
	{
		if (IC_DEBUG1 >= session_param.log_min_messages)
			LOG(DEBUG1, "status queuy message received, seq %d, srcpid %d, dstpid %d, icid %d, sid %d",
					  pkt->seq, pkt->srcPid, pkt->dstPid, pkt->icId, pkt->sessionId);

#ifdef AMS_VERBOSE_LOGGING
		logPkt("STATUS QUERY MESSAGE", pkt);
#endif
		uint32		seq = conn->conn_info.seq > 0 ? conn->conn_info.seq - 1 : 0;
		uint32		extraSeq = conn->stopRequested ? seq : conn->conn_info.extraSeq;

		conn->setAckParam(param, UDPIC_FLAGS_CAPACITY | UDPIC_FLAGS_ACK | conn->conn_info.flags, seq, extraSeq);

		return false;
	}

	/*
	 * when we're not doing a full-setup on every statement, we've got to
	 * update the peer info -- full setups do this at setup-time.
	 */

	/*
	 * Note the change here, for process start race and disordered message, if
	 * we do not fill in peer address, then we may send some acks to unknown
	 * address. Thus, the following condition is used.
	 *
	 */
	if (pkt->seq <= conn->pkt_q_capacity)
	{
		/* fill in the peer.  Need to cast away "volatile".  ugly */
		memset((void *) &conn->peer, 0, sizeof(conn->peer));
		memcpy((void *) &conn->peer, peer, *peerlen);
		conn->peer_len = *peerlen;

		conn->conn_info.dstListenerPort = pkt->dstListenerPort;
		if (IC_DEBUG2 >= session_param.log_min_messages)
			LOG(DEBUG2, "received the head packets when eliding setup, pkt seq %d", pkt->seq);
	}

	/* data packet */
	if (pkt->flags & UDPIC_FLAGS_EOS)
	{
		if (IC_DEBUG3 >= session_param.log_min_messages)
			LOG(DEBUG3, "received packet with EOS motid %d route %d seq %d",
						pkt->motNodeId, conn->route, pkt->seq);
	}

	/*
	 * if we got a stop, but didn't request a stop -- ignore, this is a
	 * startup blip: we must have acked with a stop -- we don't want to do
	 * anything further with the stop-message if we didn't request a stop!
	 *
	 * this is especially important after eliding setup is enabled.
	 */
	if (!conn->stopRequested && (pkt->flags & UDPIC_FLAGS_STOP))
	{
		if (pkt->flags & UDPIC_FLAGS_EOS)
		{
			LOG(INFO, "non-requested stop flag, EOS! seq %d, flags 0x%x", pkt->seq, pkt->flags);
		}
		return false;
	}

	if (conn->stopRequested && conn->stillActive)
	{
		if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC && IC_DEBUG5 >= session_param.log_min_messages)
			LOG(DEBUG5, "rx_thread got packet on active connection marked stopRequested. "
						"(flags 0x%x) node %d route %d pkt seq %d conn seq %d",
						pkt->flags, pkt->motNodeId, conn->route, pkt->seq, conn->conn_info.seq);

		/* can we update stillActive ? */
		if (IC_DEBUG2 >= session_param.log_min_messages)
			if (!(pkt->flags & UDPIC_FLAGS_STOP) && !(pkt->flags & UDPIC_FLAGS_EOS))
				LOG(DEBUG2, "stop requested but no stop flag on return packet ?!");

		if (pkt->flags & UDPIC_FLAGS_EOS)
			conn->conn_info.flags |= UDPIC_FLAGS_EOS;

		if (conn->conn_info.seq < pkt->seq)
			conn->conn_info.seq = pkt->seq; /* note here */

		conn->setAckParam(param, UDPIC_FLAGS_ACK | UDPIC_FLAGS_STOP | UDPIC_FLAGS_CAPACITY | conn->conn_info.flags, pkt->seq, pkt->seq);

		/* we only update stillActive if eos has been sent by peer. */
		if (pkt->flags & UDPIC_FLAGS_EOS)
		{
			if (IC_DEBUG2 >= session_param.log_min_messages)
				LOG(DEBUG2, "stop requested and acknowledged by sending peer");
			conn->stillActive = false;
		}

		return false;
	}

	/* dropped ack or timeout */
	if (pkt->seq < conn->conn_info.seq)
	{
		ic_statistics.duplicatedPktNum++;
		if (IC_DEBUG3 >= session_param.log_min_messages)
			LOG(DEBUG3, "dropped ack ? ignored data packet w/ cmd %d conn->cmd %d node %d route %d seq %d expected %d flags 0x%x",
						pkt->icId, conn->conn_info.icId, pkt->motNodeId, conn->route, pkt->seq, conn->conn_info.seq, pkt->flags);

		conn->setAckParam(param, UDPIC_FLAGS_ACK | UDPIC_FLAGS_CAPACITY | conn->conn_info.flags, conn->conn_info.seq - 1, conn->conn_info.extraSeq);

		return false;
	}

	/* sequence number is correct */
	if (!conn->stillActive)
	{
		/* peer may have dropped ack */
		if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_VERBOSE_IC &&
			IC_DEBUG1 >= session_param.log_min_messages)
			LOG(DEBUG1, "received on inactive connection node %d route %d (seq %d pkt->seq %d)",
						pkt->motNodeId, conn->route, conn->conn_info.seq, pkt->seq);

		if (conn->conn_info.seq < pkt->seq)
			conn->conn_info.seq = pkt->seq;
		conn->setAckParam(param, UDPIC_FLAGS_ACK | UDPIC_FLAGS_STOP | UDPIC_FLAGS_CAPACITY | conn->conn_info.flags, pkt->seq, pkt->seq);

		return false;
	}

	/* headSeq is the seq for the head packet. */
	uint32		headSeq = conn->conn_info.seq - conn->pkt_q_size;

	if ((conn->pkt_q_size == conn->pkt_q_capacity) || (pkt->seq - headSeq >= conn->pkt_q_capacity))
	{
		/*
		 * Error case: NO RX SPACE or out of range pkt This indicates a bug.
		 */
		logPkt("Interconnect error: received a packet when the queue is full ", pkt);
		ic_statistics.disorderedPktNum++;
		conn->stat_count_dropped++;

		if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_TIMER_IC && rx_control_info.mainWaitingState.waiting &&
			rx_control_info.mainWaitingState.waitingNode == pkt->motNodeId &&
			rx_control_info.mainWaitingState.waitingQuery == pkt->icId)
		{
			if (rx_control_info.mainWaitingState.waitingRoute == ANY_ROUTE)
			{
				if (rx_control_info.mainWaitingState.reachRoute == ANY_ROUTE)
					rx_control_info.mainWaitingState.reachRoute = conn->route;
			}
			else if (rx_control_info.mainWaitingState.waitingRoute == conn->route)
			{
				if (IC_DEBUG2 >= session_param.log_min_messages)
					LOG(INFO, "rx thread: main_waiting waking it route %d", rx_control_info.mainWaitingState.waitingRoute);
				rx_control_info.mainWaitingState.reachRoute = conn->route;
			}
			/* WAKE MAIN THREAD HERE */
			*wakeup_mainthread = true;
		}

		if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_ADVANCE_IC)
		{
			conn->setAckParam(param, UDPIC_FLAGS_FULL, conn->conn_info.seq - 1, conn->conn_info.extraSeq);
		}
		return false;
	}

	/* put the packet at the his position */
	bool		toWakeup = false;

	int			pos = (pkt->seq - 1) % conn->pkt_q_capacity;

	if (conn->pkt_q[pos] == NULL)
	{
		conn->pkt_q[pos] = (uint8 *) pkt;
		if (pos == conn->pkt_q_head)
		{
#ifdef AMS_VERBOSE_LOGGING
			LOG(INFO, "SAVE pkt at QUEUE HEAD [seq %d] for node %d route %d, queue head seq %d, queue size %d, queue head %d queue tail %d",
					  pkt->seq, pkt->motNodeId, conn->route, headSeq, conn->pkt_q_size, conn->pkt_q_head, conn->pkt_q_tail);
#endif
			toWakeup = true;
		}

		if (pos == conn->pkt_q_tail)
		{
			/* move the queue tail */
			for (; conn->pkt_q[conn->pkt_q_tail] != NULL && conn->pkt_q_size < conn->pkt_q_capacity;)
			{
				conn->pkt_q_size++;
				conn->pkt_q_tail = (conn->pkt_q_tail + 1) % conn->pkt_q_capacity;
				conn->conn_info.seq++;
			}

			/* set the EOS flag */
			if (((icpkthdr *) (conn->pkt_q[(conn->pkt_q_tail + conn->pkt_q_capacity - 1) % conn->pkt_q_capacity]))->flags & UDPIC_FLAGS_EOS)
			{
				conn->conn_info.flags |= UDPIC_FLAGS_EOS;
				if (IC_DEBUG1 >= session_param.log_min_messages)
					LOG(DEBUG1, "RX_THREAD: the packet with EOS flag is available for access in the queue for route %d", conn->route);
			}

			/* ack data packet */
			conn->setAckParam(param, UDPIC_FLAGS_CAPACITY | UDPIC_FLAGS_ACK | conn->conn_info.flags, conn->conn_info.seq - 1, conn->conn_info.extraSeq);

#ifdef AMS_VERBOSE_LOGGING
			LOG(INFO, "SAVE conn %p pkt at QUEUE TAIL [seq %d] at pos [%d] for node %d route %d, [head seq] %d, queue size %d, queue head %d queue tail %d",
					  conn, pkt->seq, pos, pkt->motNodeId, conn->route, headSeq, conn->pkt_q_size, conn->pkt_q_head, conn->pkt_q_tail);
#endif
		}
		else					/* deal with out-of-order packet */
		{
			if (IC_DEBUG1 >= session_param.log_min_messages)
				LOG(DEBUG1, "SAVE conn %p OUT-OF-ORDER pkt [seq %d] at pos [%d] for node %d route %d, [head seq] %d, queue size %d, queue head %d queue tail %d",
							conn, pkt->seq, pos, pkt->motNodeId, conn->route, headSeq, conn->pkt_q_size, conn->pkt_q_head, conn->pkt_q_tail);

			/* send an ack for out-of-order packet */
			ic_statistics.disorderedPktNum++;
			conn->handleDisorderPacket(pos, headSeq + conn->pkt_q_size, pkt);
		}
	}
	else						/* duplicate pkt */
	{
		if (IC_DEBUG1 >= session_param.log_min_messages)
			LOG(DEBUG1, "DUPLICATE pkt [seq %d], [head seq] %d, queue size %d, queue head %d queue tail %d",
						pkt->seq, headSeq, conn->pkt_q_size, conn->pkt_q_head, conn->pkt_q_tail);

		conn->setAckParam(param, UDPIC_FLAGS_DUPLICATE | conn->conn_info.flags, pkt->seq, conn->conn_info.seq - 1);
		ic_statistics.duplicatedPktNum++;
		return false;
	}

	/* Was the main thread waiting for something ? */
	if (rx_control_info.mainWaitingState.waiting &&
		rx_control_info.mainWaitingState.waitingNode == pkt->motNodeId &&
		rx_control_info.mainWaitingState.waitingQuery == pkt->icId && toWakeup)
	{
		if (rx_control_info.mainWaitingState.waitingRoute == ANY_ROUTE)
		{
			if (rx_control_info.mainWaitingState.reachRoute == ANY_ROUTE)
				rx_control_info.mainWaitingState.reachRoute = conn->route;
		}
		else if (rx_control_info.mainWaitingState.waitingRoute == conn->route)
		{
			if (IC_DEBUG2 >= session_param.log_min_messages)
				LOG(DEBUG2, "rx thread: main_waiting waking it route %d", rx_control_info.mainWaitingState.waitingRoute);
			rx_control_info.mainWaitingState.reachRoute = conn->route;
		}
		/* WAKE MAIN THREAD HERE */
		*wakeup_mainthread = true;
	}

	return true;
}

/*
 * rxThreadFunc
 * 		Main function of the receive background thread.
 *
 * NOTE: This function MUST NOT contain elog or ereport statements.
 * elog is NOT thread-safe.  Developers should instead use something like:
 *
 * NOTE: In threads, we cannot use palloc/pfree, because it's not thread safe.
 */
static void *
rxThreadFunc(void *arg)
{
	icpkthdr   *pkt = NULL;
	bool		skip_poll = false;

	for (;;)
	{
		struct pollfd nfd;
		int			n;

		/* check shutdown condition */
		if (ic_atomic_read_u32(&ic_control_info.shutdown) == 1)
		{
			if (IC_DEBUG1 >= session_param.log_min_messages)
				LOG(DEBUG1, "udp-ic: rx-thread shutting down");
			break;
		}

		/* Try to get a buffer */
		if (pkt == NULL)
		{
			pthread_mutex_lock(&ic_control_info.lock);
			pkt = rx_buffer_pool.get();
			pthread_mutex_unlock(&ic_control_info.lock);

			if (pkt == NULL)
			{
				setRxThreadError(ENOMEM);
				continue;
			}
		}

		if (!skip_poll)
		{
			/* Do we have inbound traffic to handle ? */
			nfd.fd = UDP_listenerFd;
			nfd.events = POLLIN;

			n = poll(&nfd, 1, RX_THREAD_POLL_TIMEOUT);

			if (ic_atomic_read_u32(&ic_control_info.shutdown) == 1)
			{
				if (IC_DEBUG1 >= session_param.log_min_messages)
					LOG(DEBUG1, "udp-ic: rx-thread shutting down");
				break;
			}

			if (n < 0)
			{
				if (errno == EINTR)
					continue;

				/*
				 * ERROR case: if simply break out the loop here, there will
				 * be a hung here, since main thread will never be waken up,
				 * and senders will not get responses anymore.
				 *
				 * Thus, we set an error flag, and let main thread to report
				 * an error.
				 */
				setRxThreadError(errno);
				continue;
			}

			if (n == 0)
				continue;
		}

		if (skip_poll || (n == 1 && (nfd.events & POLLIN)))
		{
			/* we've got something interesting to read */
			/* handle incoming */
			/* ready to read on our socket */
			int read_count = 0;

			struct sockaddr_storage peer;
			socklen_t	peerlen;

			peerlen = sizeof(peer);
			read_count = recvfrom(UDP_listenerFd, (char *) pkt, global_param.Gp_max_packet_size, 0,
								  (struct sockaddr *) &peer, &peerlen);

			if (ic_atomic_read_u32(&ic_control_info.shutdown) == 1)
			{
				if (IC_DEBUG1 >= session_param.log_min_messages)
					LOG(DEBUG1, "udp-ic: rx-thread shutting down");
				break;
			}

			if (IC_DEBUG5 >= session_param.log_min_messages)
				LOG(DEBUG5, "received inbound len %d", read_count);

			if (read_count < 0)
			{
				skip_poll = false;

				if (errno == EWOULDBLOCK || errno == EINTR)
					continue;

				LOG(LOG_ERROR, "Interconnect error: recvfrom (%d)", errno);

				/*
				 * ERROR case: if simply break out the loop here, there will
				 * be a hung here, since main thread will never be waken up,
				 * and senders will not get responses anymore.
				 *
				 * Thus, we set an error flag, and let main thread to report
				 * an error.
				 */
				setRxThreadError(errno);
				continue;
			}

			if (static_cast<unsigned long>(read_count) < sizeof(icpkthdr))
			{
				if (IC_DEBUG1 >= session_param.log_min_messages)
					LOG(DEBUG1, "Interconnect error: short conn receive (%d)", read_count);
				continue;
			}

			/*
			 * when we get a "good" recvfrom() result, we can skip poll()
			 * until we get a bad one.
			 */
			skip_poll = true;

			/* length must be >= 0 */
			if (pkt->len < 0)
			{
				if (IC_DEBUG3 >= session_param.log_min_messages)
					LOG(DEBUG3, "received inbound with negative length");
				continue;
			}

			if (pkt->len != static_cast<unsigned int>(read_count))
			{
				if (IC_DEBUG3 >= session_param.log_min_messages)
					LOG(DEBUG3, "received inbound packet [%d], short: read %d bytes, pkt->len %d", pkt->seq, read_count, pkt->len);
				continue;
			}

			/*
			 * check the CRC of the payload.
			 */
			if (session_param.gp_interconnect_full_crc)
			{
				if (!checkCRC(pkt))
				{
					ic_atomic_add_fetch_u32((ic_atomic_uint32 *) &ic_statistics.crcErrors, 1);
					if (IC_DEBUG2 >= session_param.log_min_messages)
						LOG(DEBUG2, "received network data error, dropping bad packet, user data unaffected.");
					continue;
				}
			}

#ifdef AMS_VERBOSE_LOGGING
			logPkt("GOT MESSAGE", pkt);
#endif

			bool		wakeup_mainthread = false;
			AckSendParam param;

			memset(&param, 0, sizeof(AckSendParam));

			/*
			 * Get the connection for the pkt.
			 *
			 * The connection hash table should be locked until finishing the
			 * processing of the packet to avoid the connection
			 * addition/removal from the hash table during the mean time.
			 */
			pthread_mutex_lock(&ic_control_info.lock);
			UDPConn *conn = ic_control_info.connHtab.find(pkt);
			if (conn != NULL)
			{
				uint64          now = getCurrentTime();
				uint64 send_time = pkt->send_time;
				uint64 recv_time = now;
				uint64 retry_times = pkt->retry_times;

				bool drop_ack = pkt->seq < conn->conn_info.seq ? true : false;
				/* Handling a regular packet */
				if (handleDataPacket(conn, pkt, &peer, &peerlen, &param, &wakeup_mainthread))
					pkt = NULL;
				if (!pkt)
				{
					param.msg.send_time = send_time;
					param.msg.recv_time = recv_time;
					param.msg.retry_times = retry_times;
				}
				if (drop_ack)
					param.msg.retry_times = session_param.Gp_interconnect_min_retries_before_timeout;
				ic_statistics.recvPktNum++;
			}
			else
			{
				/*
				 * There may have two kinds of Mismatched packets: a) Past
				 * packets from previous command after I was torn down b)
				 * Future packets from current command before my connections
				 * are built.
				 *
				 * The handling logic is to "Ack the past and Nak the future".
				 */
				if ((pkt->flags & UDPIC_FLAGS_RECEIVER_TO_SENDER) == 0)
				{
					if (IC_DEBUG1 >= session_param.log_min_messages)
						LOG(DEBUG1, "mismatched packet received, seq %d, srcpid %d, dstpid %d, icid %d, sid %d",
									pkt->seq, pkt->srcPid, pkt->dstPid, pkt->icId, pkt->sessionId);

#ifdef AMS_VERBOSE_LOGGING
					logPkt("Got a Mismatched Packet", pkt);
#endif

					if (handleMismatch(pkt, &peer, peerlen))
						pkt = NULL;
					ic_statistics.mismatchNum++;
				}
			}
			pthread_mutex_unlock(&ic_control_info.lock);

			if (wakeup_mainthread) {
				cv.notify_one();
			}

			/*
			 * real ack sending is after lock release to decrease the lock
			 * holding time.
			 */
			if (param.msg.len != 0)
				UDPConn::sendAckWithParam(&param);
		}

		/* pthread_yield(); */
	}

	/* Before return, we release the packet. */
	if (pkt)
	{
		pthread_mutex_lock(&ic_control_info.lock);
		rx_buffer_pool.release(pkt);
		pkt = NULL;
		pthread_mutex_unlock(&ic_control_info.lock);
	}

	/* nothing to return */
	return NULL;
}

/*
 * handleMismatch
 * 		If the mismatched packet is from an old connection, we may need to
 * 		send an acknowledgment.
 *
 * We are called with the receiver-lock held, and we never release it.
 *
 * For QD:
 * 1) Not in hashtable     : NAK it/Do nothing
 * 	  Causes:  a) Start race
 * 	           b) Before the entry for the ic instance is inserted, an error happened.
 * 	           c) From past transactions: should no happen.
 * 2) Active in hashtable  : NAK it/Do nothing
 *    Causes:  a) Error reported after the entry is inserted, and connections are
 *                not inserted to the hashtable yet, and before teardown is called.
 * 3) Inactive in hashtable: ACK it (with stop)
 *    Causes: a) Normal execution: after teardown is called on current command.
 *            b) Error case, 2a) after teardown is called.
 *            c) Normal execution: from past history transactions (should not happen).
 *
 * For QE:
 * 1) pkt->id > ic_control_info.ic_instance_id : NAK it/Do nothing
 *    Causes: a) Start race
 *            b) Before ic_control_info.ic_instance_id is assigned to correct value, an error happened.
 * 2) lastTornIcId < pkt->id == ic_control_info.ic_instance_id: NAK it/Do nothing
 *    Causes:  a) Error reported after ic_control_info.ic_instance_id is set, and connections are
 *                not inserted to the hashtable yet, and before teardown is called.
 * 3) lastTornIcId == pkt->id == ic_control_info.ic_instance_id: ACK it (with stop)
 *    Causes:  a) Normal execution: after teardown is called on current command
 * 4) pkt->id < ic_control_info.ic_instance_id: NAK it/Do nothing/ACK it.
 *    Causes:  a) Should not happen.
 *
 */
static bool
handleMismatch(icpkthdr *pkt, struct sockaddr_storage *peer, int peer_len)
{
	bool		cached = false;

	/*
	 * we want to ack old packets; but *must* avoid acking connection
	 * requests:
	 *
	 * "ACK the past, NAK the future" explicit NAKs aren't necessary, we just
	 * don't want to ACK future packets, that confuses everyone.
	 */
	if (pkt->seq > 0 && pkt->sessionId == session_param.gp_session_id)
	{
		bool		need_ack = false;
		uint8		ack_flags = 0;

		/*
		 * The QD-backends can't use a counter, they've potentially got
		 * multiple instances (one for each active cursor)
		 */
		if (global_param.Gp_role == GP_ROLE_DISPATCH_IC)
		{
			struct CursorICHistoryEntry *p;

			p = rx_control_info.cursorHistoryTable.get(pkt->icId);
			if (p)
			{
				if (p->status == 0)
				{
					/* Torn down. Ack the past. */
					need_ack = true;
				}
				else			/* p->status == 1 */
				{
					/*
					 * Not torn down yet. It happens when an error
					 * (out-of-memory, network error...) occurred after the
					 * cursor entry is inserted into the table in interconnect
					 * setup process. The peer will be canceled.
					 */
					if (IC_DEBUG1 >= session_param.log_min_messages)
						LOG(DEBUG1, "GOT A MISMATCH PACKET WITH ID %d HISTORY THINKS IT IS ACTIVE", pkt->icId);
					return cached;	/* ignore, no ack */
				}
			}
			else
			{
				if (IC_DEBUG1 >= session_param.log_min_messages)
					LOG(DEBUG1, "GOT A MISMATCH PACKET WITH ID %d HISTORY HAS NO RECORD", pkt->icId);

				/*
				 * No record means that two possibilities. 1) It is from the
				 * future. It is due to startup race. We do not ack future
				 * packets 2) Before the entry for the ic instance is
				 * inserted, an error happened. We do not ack for this case
				 * too. The peer will be canceled.
				 */
				ack_flags = UDPIC_FLAGS_NAK;
				need_ack = false;

				if (session_param.gp_interconnect_cache_future_packets)
				{
					cached = cacheFuturePacket(pkt, peer, peer_len);
				}
			}
		}
		/* The QEs get to use a simple counter. */
		else if (global_param.Gp_role == GP_ROLE_EXECUTE_IC)
		{
			if (ic_control_info.ic_instance_id >= pkt->icId)
			{
				need_ack = true;

				/*
				 * We want to "ACK the past, but NAK the future."
				 *
				 * handleAck() will retransmit.
				 */
				if (pkt->seq >= 1 && pkt->icId > rx_control_info.lastTornIcId)
				{
					ack_flags = UDPIC_FLAGS_NAK;
					need_ack = false;
				}
			}
			else
			{
				/*
				 * ic_control_info.ic_instance_id < pkt->icId, from the future
				 */ 
				if (session_param.gp_interconnect_cache_future_packets)
				{
					cached = cacheFuturePacket(pkt, peer, peer_len);
				}
			}
		}

		if (need_ack)
		{
			UDPConn	dummyconn(NULL);
			char		buf[128];	/* numeric IP addresses shouldn't exceed
									 * about 50 chars, but play it safe */

			memcpy(&dummyconn.conn_info, pkt, sizeof(icpkthdr));
			dummyconn.peer = *peer;
			dummyconn.peer_len = peer_len;

			dummyconn.conn_info.flags |= ack_flags;

			if (IC_DEBUG1 >= session_param.log_min_messages)
				LOG(DEBUG1, "ACKING PACKET WITH FLAGS: pkt->seq %d 0x%x [pkt->icId %d last-teardown %d interconnect_id %d]",
							pkt->seq, dummyconn.conn_info.flags, pkt->icId, rx_control_info.lastTornIcId, ic_control_info.ic_instance_id);

			format_sockaddr_udp(&dummyconn.peer, buf, sizeof(buf));

			if (IC_DEBUG1 >= session_param.log_min_messages)
				LOG(DEBUG1, "ACKING PACKET TO %s", buf);

			if ((ack_flags & UDPIC_FLAGS_NAK) == 0)
			{
				ack_flags |= UDPIC_FLAGS_STOP | UDPIC_FLAGS_ACK | UDPIC_FLAGS_CAPACITY | UDPIC_FLAGS_RECEIVER_TO_SENDER;
			}
			else
			{
				ack_flags |= UDPIC_FLAGS_RECEIVER_TO_SENDER;
			}

			/*
			 * There are two cases, we may need to send a response to sender
			 * here. One is start race and the other is receiver becomes idle.
			 *
			 * ack_flags here can take two possible values 1) UDPIC_FLAGS_NAK
			 * | UDPIC_FLAGS_RECEIVER_TO_SENDER (for start race) 2)
			 * UDPIC_FLAGS_STOP | UDPIC_FLAGS_ACK | UDPIC_FLAGS_CAPACITY |
			 * UDPIC_FLAGS_RECEIVER_TO_SENDER (for idle receiver)
			 *
			 * The final flags in the packet may take some extra bits such as
			 * 1) UDPIC_FLAGS_STOP 2) UDPIC_FLAGS_EOS 3) UDPIC_FLAGS_CAPACITY
			 * which are from original packet
			 */
			dummyconn.sendAck(ack_flags | dummyconn.conn_info.flags, dummyconn.conn_info.seq, dummyconn.conn_info.seq);
		}
	}
	else
	{
		if (IC_DEBUG1 >= session_param.log_min_messages)
			LOG(DEBUG1, "dropping packet from command-id %d seq %d (my cmd %d)", pkt->icId, pkt->seq, ic_control_info.ic_instance_id);
	}

	return cached;
}

/*
 * cacheFuturePacket
 *		Cache the future packets during the setupUDPIFCInterconnect.
 *
 * Return true if packet is cached, otherwise false
 */
static bool
cacheFuturePacket(icpkthdr *pkt, struct sockaddr_storage *peer, int peer_len)
{
	UDPConn *conn = ic_control_info.startupCacheHtab.find(pkt);
	if (conn == NULL)
	{
		try {
			conn = new UDPConn(NULL);
		} catch (const std::bad_alloc & e) {
			errno = ENOMEM;
			setRxThreadError(errno);
			return false;
		}

		memset((void *) conn, 0, sizeof(UDPConn));
		memcpy(&conn->conn_info, pkt, sizeof(icpkthdr));

		conn->pkt_q_capacity = session_param.Gp_interconnect_queue_depth;
		conn->pkt_q_size = session_param.Gp_interconnect_queue_depth;
		conn->pkt_q = (uint8 **) ic_malloc(session_param.Gp_interconnect_queue_depth * sizeof(uint8 *));

		if (conn->pkt_q == NULL)
		{
			/* malloc failed.  */
			delete conn;
			setRxThreadError(errno);
			return false;
		}

		/* We only use the array to store cached packets. */
		memset(conn->pkt_q, 0, session_param.Gp_interconnect_queue_depth * sizeof(uint8 *));

		/* Put connection to the hashtable. */
		if (!ic_control_info.startupCacheHtab.add(conn))
		{
			ic_free(conn->pkt_q);
			delete conn;
			setRxThreadError(errno);
			return false;
		}

		/* Setup the peer sock information. */
		memcpy(&conn->peer, peer, peer_len);
		conn->peer_len = peer_len;
	}

	/*
	 * Reject packets with invalid sequence numbers and packets which have
	 * been cached before.
	 */
	if (pkt->seq > conn->pkt_q_size || pkt->seq == 0 || conn->pkt_q[pkt->seq - 1] != NULL)
		return false;

	conn->pkt_q[pkt->seq - 1] = (uint8 *) pkt;
	rx_buffer_pool.maxCount++;
	ic_statistics.startupCachedPktNum++;

	return true;
}

/*
 * cleanupStartupCache
 *		Clean the startup cache.
 */
static void
cleanupStartupCache()
{
	ConnHtabBin *bin = NULL;
	UDPConn *cachedConn = NULL;
	icpkthdr   *pkt = NULL;
	int			i = 0;
	uint32		j = 0;

	for (i = 0; i < ic_control_info.startupCacheHtab.size; i++)
	{
		bin = ic_control_info.startupCacheHtab.table[i];

		while (bin)
		{
			cachedConn = bin->conn;

			for (j = 0; j < cachedConn->pkt_q_size; j++)
			{
				pkt = (icpkthdr *) cachedConn->pkt_q[j];

				if (pkt == NULL)
					continue;

				rx_buffer_pool.maxCount--;
				rx_buffer_pool.put(pkt);
				cachedConn->pkt_q[j] = NULL;
			}
			bin = bin->next;
			ic_control_info.startupCacheHtab.remove(cachedConn);

			/*
			 * MPP-19981 free the cached connections; otherwise memory leak
			 * would be introduced.
			 */
			ic_free(cachedConn->pkt_q);
			ic_free(cachedConn);
		}
	}
}


#ifdef USE_ASSERT_CHECKING

/* The following functions are facility methods for debugging.
 * They are quite useful when there are a large number of connections.
 * These functions can be called from gdb to output internal information to a file.
 */

/*
 * dumpUnackQueueRing
 * 		Dump an unack queue ring.
 */
static void
dumpUnackQueueRing(const char *fname)
{
	FILE	   *ofile = fopen(fname, "w+");
	int			i;

	fprintf(ofile, "UnackQueueRing: currentTime %lu, idx %d numOutstanding %d numSharedOutstanding %d\n",
			unack_queue_ring.currentTime, unack_queue_ring.idx,
			unack_queue_ring.numOutStanding, unack_queue_ring.numSharedOutStanding);
	fprintf(ofile, "==================================\n");
	for (i = 0; i < UNACK_QUEUE_RING_SLOTS_NUM; i++)
	{
		if (unack_queue_ring.slots[i].length() > 0)
		{
			unack_queue_ring.slots[i].dump_to_file(ofile);
		}
	}

	fclose(ofile);
}

/*
 * dumpConnections
 * 		Dump connections.
 */
void
TransportEntry::dumpConnections(const char *fname)
{
	int			i;
	uint32		j;

	return;

	FILE	   *ofile = fopen(fname, "w+");

	fprintf(ofile, "Entry connections: conn num %d \n", this->numConns);
	fprintf(ofile, "==================================\n");
	for (i = 0; i < this->numConns; i++)
	{
		UDPConn *conn = this->GetConn(i);

		fprintf(ofile, "conns[%d] motNodeId=%d: remoteContentId=%d pid=%d sockfd=%d remote=%s "
				"capacity=%d sentSeq=%d receivedAckSeq=%d consumedSeq=%d rtt=%lu"
				" dev=%lu deadlockCheckBeginTime=%lu route=%d msgSize=%d msgPos=%p"
				" recvBytes=%d tupleCount=%d stillActive=%d stopRequested=%d "
				"state=%d\n",
				i, this->motNodeId,
				conn->remoteContentId,
				conn->cdbProc ? conn->cdbProc->pid : 0,
				conn->sockfd,
				conn->remoteHostAndPort,
				conn->capacity, conn->sentSeq, conn->receivedAckSeq, conn->consumedSeq,
				conn->rtt, conn->dev, conn->deadlockCheckBeginTime, conn->route, conn->msgSize, conn->msgPos,
				conn->recvBytes, conn->tupleCount, conn->stillActive, conn->stopRequested,
				conn->state);
		fprintf(ofile, "conn_info [%s: seq %d extraSeq %d]: motNodeId %d, crc %d len %d "
				"srcContentId %d dstDesContentId %d "
				"srcPid %d dstPid %d "
				"srcListenerPort %d dstListernerPort %d "
				"sendSliceIndex %d recvSliceIndex %d "
				"sessionId %d icId %d "
				"flags %d\n",
				conn->conn_info.flags & UDPIC_FLAGS_RECEIVER_TO_SENDER ? "ACK" : "DATA",
				conn->conn_info.seq, conn->conn_info.extraSeq, conn->conn_info.motNodeId, conn->conn_info.crc, conn->conn_info.len,
				conn->conn_info.srcContentId, conn->conn_info.dstContentId,
				conn->conn_info.srcPid, conn->conn_info.dstPid,
				conn->conn_info.srcListenerPort, conn->conn_info.dstListenerPort,
				conn->conn_info.sendSliceIndex, conn->conn_info.recvSliceIndex,
				conn->conn_info.sessionId, conn->conn_info.icId,
				conn->conn_info.flags);

		if (!ic_control_info.isSender)
		{
			fprintf(ofile, "pkt_q_size=%d pkt_q_head=%d pkt_q_tail=%d pkt_q=%p\n", conn->pkt_q_size, conn->pkt_q_head, conn->pkt_q_tail, conn->pkt_q);
			for (j = 0; j < conn->pkt_q_capacity; j++)
			{
				if (conn->pkt_q != NULL && conn->pkt_q[j] != NULL)
				{
					icpkthdr   *pkt = (icpkthdr *) conn->pkt_q[j];

					fprintf(ofile, "Packet (pos %d) Info [%s: seq %d extraSeq %d]: motNodeId %d, crc %d len %d "
							"srcContentId %d dstDesContentId %d "
							"srcPid %d dstPid %d "
							"srcListenerPort %d dstListernerPort %d "
							"sendSliceIndex %d recvSliceIndex %d "
							"sessionId %d icId %d "
							"flags %d\n",
							j,
							pkt->flags & UDPIC_FLAGS_RECEIVER_TO_SENDER ? "ACK" : "DATA",
							pkt->seq, pkt->extraSeq, pkt->motNodeId, pkt->crc, pkt->len,
							pkt->srcContentId, pkt->dstContentId,
							pkt->srcPid, pkt->dstPid,
							pkt->srcListenerPort, pkt->dstListenerPort,
							pkt->sendSliceIndex, pkt->recvSliceIndex,
							pkt->sessionId, pkt->icId,
							pkt->flags);
				}
			}
		}
		if (ic_control_info.isSender)
		{
			fprintf(ofile, "sndQueue ");
			conn->sndQueue.dump_to_file(ofile);
			fprintf(ofile, "unackQueue ");
			conn->unackQueue.dump_to_file(ofile);

			dumpUnackQueueRing("/tmp/dumpUnackQueueRing");
		}
		fprintf(ofile, "\n");
	}
	fclose(ofile);
}
#endif

/*
 * logPkt
 * 		Log a packet.
 *
 */
static inline void
logPkt(const char *prefix, icpkthdr *pkt)
{
	LOG(INFO, "%s [%s: seq %d extraSeq %d]: motNodeId %d, crc %d len %d "
			  "srcContentId %d dstDesContentId %d "
			  "srcPid %d dstPid %d "
			  "srcListenerPort %d dstListernerPort %d "
			  "sendSliceIndex %d recvSliceIndex %d "
			  "sessionId %d icId %d "
			  "flags %d ",
			  prefix, pkt->flags & UDPIC_FLAGS_RECEIVER_TO_SENDER ? "ACK" : "DATA",
			  pkt->seq, pkt->extraSeq, pkt->motNodeId, pkt->crc, pkt->len,
			  pkt->srcContentId, pkt->dstContentId,
			  pkt->srcPid, pkt->dstPid,
			  pkt->srcListenerPort, pkt->dstListenerPort,
			  pkt->sendSliceIndex, pkt->recvSliceIndex,
			  pkt->sessionId, pkt->icId,
			  pkt->flags);
}

/*
 * Send a dummy packet to interconnect thread to exit poll() immediately
 */
static void
SendDummyPacket(void)
{
   int                 ret;
   const char          *dummy_pkt = "stop it";
   int                 counter;
   struct sockaddr_storage dest;
   socklen_t   dest_len;

   Assert(udp_dummy_packet_sockaddr.ss_family == AF_INET || udp_dummy_packet_sockaddr.ss_family == AF_INET6);
   Assert(ICSenderFamily == AF_INET || ICSenderFamily == AF_INET6);

   dest = udp_dummy_packet_sockaddr;
   dest_len = (ICSenderFamily == AF_INET) ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6);

   if (ICSenderFamily == AF_INET6)
   {
#if defined(__darwin__)
       if (udp_dummy_packet_sockaddr.ss_family == AF_INET6)
           ConvertIPv6WildcardToLoopback(&dest);
#endif
       if (udp_dummy_packet_sockaddr.ss_family == AF_INET)
           ConvertToIPv4MappedAddr(&dest, &dest_len);
   }

   if (ICSenderFamily == AF_INET && udp_dummy_packet_sockaddr.ss_family == AF_INET6)
   {
       /* the size of AF_INET6 is bigger than the side of IPv4, so
        * converting from IPv6 to IPv4 may potentially not work. */
       LOG(INFO, "sending dummy packet failed: cannot send from AF_INET to receiving on AF_INET6");
       return;
   }

	/*
     * Send a dummy package to the interconnect listener, try 10 times.
     * We don't want to close the socket at the end of this function, since
     * the socket will eventually close during the motion layer cleanup.
	 */

	counter = 0;
	while (counter < 10)
	{
		counter++;
		ret = sendto(ICSenderSocket, dummy_pkt, strlen(dummy_pkt), 0, (struct sockaddr *) &dest, dest_len);
		if (ret < 0)
		{
			if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
				continue;
			else
			{
				LOG(INFO, "send dummy packet failed, sendto failed: %m");
				return;
			}
		}
		break;
	}

	if (counter >= 10)
	{
		LOG(INFO, "send dummy packet failed, sendto failed with 10 times: %m");
	}
}

/*
 * prepareXmit
 * 		Prepare connection for transmit.
 */
void
UDPConn::prepareXmit()
{
	this->conn_info.len = this->msgSize;
	this->conn_info.crc = 0;

	memcpy(this->pBuff, &this->conn_info, sizeof(this->conn_info));

	/* increase the sequence no */
	this->conn_info.seq++;

	if (session_param.gp_interconnect_full_crc)
	{
		icpkthdr *pkt = (icpkthdr *)this->pBuff;
		addCRC(pkt);
	}
}

 /*
  * sendtoWithRetry
  *         Retry sendto logic and send the packets.
  */
static ssize_t
sendtoWithRetry(int socket, const void *message, size_t length,
           int flags, const struct sockaddr *dest_addr,
           socklen_t dest_len, int retry, const char *errDetail)
{
    int32       n;
    int count = 0;

xmit_retry:
    /*
     * If given retry count is positive, retry up to the limited times.
     * Otherwise, retry for unlimited times until succeed.
     */
    if (retry > 0 && ++count > retry)
       return n;
	n = sendto(socket, message, length, flags, dest_addr, dest_len);
    if (n < 0)
    {
		int			save_errno = errno;

		if (errno == EINTR)
			goto xmit_retry;

        /*
         * EAGAIN: no space ? not an error.
         *
         * EFAULT: In Linux system call, it only happens when copying a socket
         * address into kernel space failed, which is less likely to happen,
         * but mocked heavily by our fault injection in regression tests.
         */
        if (errno == EAGAIN || errno == EFAULT)
            return n;

		/*
		 * If Linux iptables (nf_conntrack?) drops an outgoing packet, it may
		 * return an EPERM to the application. This might be simply because of
		 * traffic shaping or congestion, so ignore it.
		 */
		if (errno == EPERM)
		{
			LOG(LOG_ERROR, "Interconnect error writing an outgoing packet: %m, "
						   "error during sendto() %s", errDetail);
			return n;
		}

		/*
		 * If the OS can detect an MTU issue on the host network interfaces, we
		 * would get EMSGSIZE here. So, bail with a HINT about checking MTU.
		 */
		if (errno == EMSGSIZE)
		{
			std::stringstream ss;
			ss << "ERROR, Interconnect error writing an outgoing packet: " << strerror(errno) << "error during sendto() call (error:" << save_errno << ", " << errDetail << ")."
			   << "check if interface MTU is equal across the cluster and lower than gp_max_packet_size" << "\n";
			throw ICNetworkException(ss.str(), __FILE__, __LINE__);
		}

		std::stringstream ss;
		ss <<"ERROR, Interconnect error writing an outgoing packet: "<<strerror(errno)<<
			"error during sendto() call (error:"<<save_errno<<", "<<errDetail << ").\n";
		throw ICNetworkException(ss.str(), __FILE__, __LINE__);
		/* not reached */
	}

	return n;
}

/*
 * sendOnce
 * 		Send a packet.
 */
void
UDPConn::sendOnce(icpkthdr *pkt)
{
	int32 n;
#ifdef USE_ASSERT_CHECKING
	if (testmode_inject_fault(session_param.gp_udpic_dropxmit_percent))
	{
#ifdef AMS_VERBOSE_LOGGING
		LOG(INFO, "THROW PKT with seq %d srcpid %d despid %d",
				  pkt->seq, pkt->srcPid, pkt->dstPid);
#endif
		return;
	}
#endif

	Assert(pkt->srcContentId == global_param.segindex);
	Assert(pkt->motNodeId == entry_->motNodeId);
	LOG(DEBUG3, "UDPConn::sendOnce(): icid: %d, motNodeId: %d, srcSeg: %d, dstSeg: %d, srcPid: %d, dstPid: %d, seq: %d, len: %d, flags: %s",
			pkt->icId, pkt->motNodeId, pkt->srcContentId, pkt->dstContentId, pkt->srcPid, pkt->dstPid, pkt->seq, pkt->len, flags2txt(pkt->flags));

    char errDetail[256];
    snprintf(errDetail, sizeof(errDetail), "For Remote Connection: contentId=%d at %s",
                      this->remoteContentId,
                      this->remoteHostAndPort);
    n = sendtoWithRetry(this->entry_->txfd, pkt, pkt->len, 0,
                           (struct sockaddr *) &this->peer, this->peer_len, -1, errDetail);
	if (n != int(pkt->len))
	{
		if (IC_DEBUG1 >= session_param.log_min_messages)
			LOG(DEBUG1, "Interconnect error writing an outgoing packet [seq %d]: short transmit (given %d sent %d) during sendto() call."
						"For Remote Connection: contentId=%d at %s", pkt->seq, pkt->len, n,
						this->remoteContentId, this->remoteHostAndPort);
#ifdef AMS_VERBOSE_LOGGING
		logPkt("PKT DETAILS ", pkt);
#endif
	}
	return;
}

void
UDPConn::handleStop()
{
	if (!this->stillActive || !this->stopRequested)
		return;

	/* mark buffer empty */
	this->tupleCount = 0;
	this->msgSize = sizeof(this->conn_info);

	/* now send our stop-ack EOS */
	this->conn_info.flags |= UDPIC_FLAGS_EOS;

	Assert(this->curBuff != NULL);

	this->pBuff[this->msgSize] = 'S';
	this->msgSize += 1;

	/* now ready to actually send */
	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
		LOG(DEBUG1, "handleStopMsgs: node %d route %d, seq %d",
					entry_->motNodeId, this->route, this->conn_info.seq);

	/* place it into the send queue */
	this->prepareXmit();
	this->sndQueue.append(this->curBuff);
	this->curBuff = NULL;
	this->pBuff = NULL;

	/* return all buffers */
	this->sndQueue.release(false);
	this->unackQueue.release(session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_CAPACITY_IC ? false : true);

	this->tupleCount = 0;
	this->msgSize = sizeof(this->conn_info);

	this->state = mcsEosSent;
	this->stillActive = false;
	this->stopRequested = false;
}

/*
 * sendBuffers
 * 		Called by sender to send the buffers in the send queue.
 *
 * Send the buffers in the send queue of the connection if there is capacity left
 * and the congestion control condition is satisfied.
 *
 * Here, we make sure that a connection can have at least one outstanding buffer.
 * This is very important for two reasons:
 *
 * 1) The handling logic of the ack of the outstanding buffer can always send a buffer
 *    in the send queue. Otherwise, there may be a deadlock.
 * 2) This makes sure that any connection can have a minimum bandwidth for data
 *    sending.
 *
 * After sending a buffer, the buffer will be placed into both the unack queue and
 * the corresponding queue in the unack queue ring.
 */
void
UDPConn::sendBuffers()
{
	while (this->capacity > 0 && this->sndQueue.length() > 0)
	{
		ICBuffer   *buf = NULL;

		if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_IC || session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_ADVANCE_IC)
		{
			if (this->unackQueue.length() > 0 &&
				unack_queue_ring.numSharedOutStanding >= (snd_control_info.cwnd - snd_control_info.minCwnd))
			break;
		}

		/* for connection setup, we only allow one outstanding packet. */
		if (this->state == mcsSetupOutgoingConnection && this->unackQueue.length() >= 1)
			break;

		buf = this->sndQueue.pop();

		uint64		now = getCurrentTime();

		buf->sentTime = now;
		buf->unackQueueRingSlot = -1;
		buf->nRetry = 0;
		buf->conn = this;
		this->capacity--;

		this->unackQueue.append(buf);

		if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_IC || session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_ADVANCE_IC)
		{
			unack_queue_ring.numOutStanding++;
			if (this->unackQueue.length() > 1)
				unack_queue_ring.numSharedOutStanding++;

			putIntoUnackQueueRing(&unack_queue_ring,
								  buf,
								  this->computeExpirationPeriod(buf->nRetry),
								  now);
		}

		/*
		 * Note the place of sendOnce here. If we send before appending it to
		 * the unack queue and putting it into unack queue ring, and there is
		 * a network error occurred in the sendOnce function, error message
		 * will be output. In the time of error message output, interrupts is
		 * potentially checked, if there is a pending query cancel, it will
		 * lead to a dangled buffer (memory leak).
		 */
#ifdef TRANSFER_PROTOCOL_STATS
		trans_proto_stats.update(TPE_DATA_PKT_SEND, buf->pkt);
#endif

		struct icpkthdr *pkt_ = buf->pkt;
		pkt_->send_time = now;
		pkt_->recv_time = 0;
		pkt_->retry_times = buf->nRetry;
		this->sendOnce(buf->pkt);

		ic_statistics.sndPktNum++;

#ifdef AMS_VERBOSE_LOGGING
		logPkt("SEND PKT DETAIL", buf->pkt);
#endif

		this->sentSeq = buf->pkt->seq;
	}
}

/*
 * handleDisorderPacket
 * 		Called by rx thread to assemble and send a disorder message.
 *
 * In current implementation, we limit the number of lost packet sequence numbers
 * in the disorder message by the MIN_PACKET_SIZE. There are two reasons here:
 *
 * 1) The maximal number of lost packet sequence numbers are actually bounded by the
 *    receive queue depth whose maximal value is very large. Since we share the packet
 *    receive and ack receive in the background thread, the size of disorder should be
 *    also limited by the max packet size.
 * 2) We can use Gp_max_packet_size here to limit the number of lost packet sequence numbers.
 *    But considering we do not want to let senders send many packets when getting a lost
 *    message. Here we use MIN_PACKET_SIZE.
 *
 *
 * the format of a disorder message:
 * I) pkt header
 *  - seq      -> packet sequence number that triggers the disorder message
 *  - extraSeq -> the largest seq of the received packets
 *  - flags    -> UDPIC_FLAGS_DISORDER
 *  - len      -> sizeof(icpkthdr) + sizeof(uint32) * (lost pkt count)
 * II) content
 *  - an array of lost pkt sequence numbers (uint32)
 *
 */
void
UDPConn::handleDisorderPacket(int pos, uint32 tailSeq, icpkthdr *pkt)
{
	int			start = 0;
	uint32		lostPktCnt = 0;
	uint32	   *curSeq = (uint32 *) &rx_control_info.disorderBuffer[1];
	uint32		maxSeqs = MAX_SEQS_IN_DISORDER_ACK;

#ifdef AMS_VERBOSE_LOGGING
	LOG(INFO, "PROCESS_DISORDER PKT BEGIN:");
#endif

	start = this->pkt_q_tail;

	while (start != pos && lostPktCnt < maxSeqs)
	{
		if (this->pkt_q[start] == NULL)
		{
			*curSeq = tailSeq;
			lostPktCnt++;
			curSeq++;

#ifdef AMS_VERBOSE_LOGGING
			LOG(INFO, "PROCESS_DISORDER add seq [%d], lostPktCnt %d", *curSeq, lostPktCnt);
#endif
		}

		tailSeq++;
		start = (start + 1) % this->pkt_q_capacity;
	}

#ifdef AMS_VERBOSE_LOGGING
	LOG(INFO, "PROCESS_DISORDER PKT END:");
#endif

	/* when reaching here, cnt must not be 0 */
	this->sendDisorderAck(pkt->seq, this->conn_info.seq - 1, lostPktCnt);
}

/*
 * handleAckForDisorderPkt
 * 		Called by sender to deal with acks for disorder packet.
 */
bool
UDPConn::handleAckForDisorderPkt(icpkthdr *pkt)
{
	ICBufferLink *link = NULL;
	ICBuffer   *buf = NULL;
	ICBufferLink *next = NULL;
	uint64		now = getCurrentTime();
	uint32	   *curLostPktSeq = 0;
	int			lostPktCnt = 0;
	static uint32 times = 0;
	static uint32 lastSeq = 0;
	bool		shouldSendBuffers = false;

	if (pkt->extraSeq != lastSeq)
	{
		lastSeq = pkt->extraSeq;
		times = 0;
		return false;
	}
	else
	{
		times++;
		if (times != 2)
			return false;
	}

	curLostPktSeq = (uint32 *) &pkt[1];
	lostPktCnt = (pkt->len - sizeof(icpkthdr)) / sizeof(uint32);

	/*
	 * Resend all the missed packets and remove received packets from queues
	 */

	link = this->unackQueue.first();
	buf = GET_ICBUFFER_FROM_PRIMARY(link);

#ifdef AMS_VERBOSE_LOGGING
	LOG(INFO, "DISORDER: pktlen %d cnt %d pktseq %d first loss %d buf %p",
			  pkt->len, lostPktCnt, pkt->seq, *curLostPktSeq, buf);

	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
	{
		this->unackQueue.icBufferListLog();
		this->sndQueue.icBufferListLog();
	}
#endif

	/*
	 * iterate the unack queue
	 */
	while (!this->unackQueue.is_head(link) && buf->pkt->seq <= pkt->seq && lostPktCnt > 0)
	{
#ifdef AMS_VERBOSE_LOGGING
		LOG(INFO, "DISORDER: bufseq %d curlostpkt %d cnt %d buf %p pkt->seq %d",
				  buf->pkt->seq, *curLostPktSeq, lostPktCnt, buf, pkt->seq);
#endif

		if (buf->pkt->seq == pkt->seq)
		{
			this->handleAckedPacket(buf, now, pkt);
			shouldSendBuffers = true;
			break;
		}

		if (buf->pkt->seq == *curLostPktSeq)
		{
			/* this is a lost packet, retransmit */

			buf->nRetry++;
			if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_IC || session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_ADVANCE_IC)
			{
				ICBufferList *alist = &unack_queue_ring.slots[buf->unackQueueRingSlot];
				buf = alist->remove(buf);
				putIntoUnackQueueRing(&unack_queue_ring, buf,
									  this->computeExpirationPeriod(buf->nRetry), now);
			}
#ifdef TRANSFER_PROTOCOL_STATS
			trans_proto_stats.update(TPE_DATA_PKT_SEND, buf->pkt);
#endif

			Assert(this == buf->conn);
			this->sendOnce(buf->pkt);

#ifdef AMS_VERBOSE_LOGGING
			LOG(INFO, "RESEND a buffer for DISORDER: seq %d", buf->pkt->seq);
			logPkt("DISORDER RESEND DETAIL ", buf->pkt);
#endif

			ic_statistics.retransmits++;
			curLostPktSeq++;
			lostPktCnt--;

			link = link->next;
			buf = GET_ICBUFFER_FROM_PRIMARY(link);
		}
		else if (buf->pkt->seq < *curLostPktSeq)
		{
			/* remove packet already received. */

			next = link->next;
			this->handleAckedPacket(buf, now, pkt);
			shouldSendBuffers = true;
			link = next;
			buf = GET_ICBUFFER_FROM_PRIMARY(link);
		}
		else					/* buf->pkt->seq > *curPktSeq */
		{
			/*
			 * this case is introduced when the disorder message tell you a
			 * pkt is lost. But when we handle this message, a message (for
			 * example, duplicate ack, or another disorder message) arriving
			 * before this message already removed the pkt.
			 */
			curLostPktSeq++;
			lostPktCnt--;
		}
	}
	if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_IC || session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_ADVANCE_IC)
	{
		snd_control_info.ssthresh = Max(snd_control_info.cwnd / 2, snd_control_info.minCwnd);
		snd_control_info.cwnd = snd_control_info.ssthresh;
	}
#ifdef AMS_VERBOSE_LOGGING
	LOG(INFO, "After DISORDER: sndQ %d unackQ %d", this->sndQueue.length(), this->unackQueue.length());
	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
	{
		this->unackQueue.icBufferListLog();
		this->sndQueue.icBufferListLog();
	}
#endif

	return shouldSendBuffers;
}

/*
 * handleAckForDuplicatePkt
 * 		Called by sender to deal with acks for duplicate packet.
 *
 */
bool
UDPConn::handleAckForDuplicatePkt(icpkthdr *pkt)
{
	ICBufferLink *link = NULL;
	ICBuffer   *buf = NULL;
	ICBufferLink *next = NULL;
	uint64		now = getCurrentTime();
	bool		shouldSendBuffers = false;

#ifdef AMS_VERBOSE_LOGGING
	LOG(INFO, "RESEND the unacked buffers in the queue due to %s", pkt->len == 0 ? "PROCESS_START_RACE" : "DISORDER");
#endif

	if (pkt->seq <= pkt->extraSeq)
	{
		/* Indicate a bug here. */
		LOG(LOG_ERROR, "invalid duplicate message: seq %d extraSeq %d", pkt->seq, pkt->extraSeq);
		return false;
	}

	link = this->unackQueue.first();
	buf = GET_ICBUFFER_FROM_PRIMARY(link);

	/* deal with continuous pkts */
	while (!this->unackQueue.is_head(link) && (buf->pkt->seq <= pkt->extraSeq))
	{
		next = link->next;
		this->handleAckedPacket(buf, now, pkt);
		shouldSendBuffers = true;
		link = next;
		buf = GET_ICBUFFER_FROM_PRIMARY(link);
	}

	/* deal with the single duplicate packet */
	while (!this->unackQueue.is_head(link) && buf->pkt->seq <= pkt->seq)
	{
		next = link->next;
		if (buf->pkt->seq == pkt->seq)
		{
			this->handleAckedPacket(buf, now, pkt);
			shouldSendBuffers = true;
			break;
		}
		link = next;
		buf = GET_ICBUFFER_FROM_PRIMARY(link);
	}

	return shouldSendBuffers;
}

/*
 * checkNetworkTimeout
 *		check network timeout case.
 */
void
UDPConn::checkNetworkTimeout(ICBuffer *buf, uint64 now, bool *networkTimeoutIsLogged)
{
	/*
	 * Using only the time to first sent time to decide timeout is not enough,
	 * since there is a possibility the sender process is not scheduled or
	 * blocked by OS for a long time. In this case, only a few times are
	 * tried. Thus, the GUC Gp_interconnect_min_retries_before_timeout is
	 * added here.
	 */
	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC &&
		buf->nRetry % session_param.Gp_interconnect_debug_retry_interval == 0)
	{
		LOG(INFO, "resending packet (seq %d) to %s (pid %d cid %d) with %d retries in %lu seconds",
				  buf->pkt->seq, buf->conn->remoteHostAndPort, buf->pkt->dstPid, buf->pkt->dstContentId, buf->nRetry,
				  (now - buf->sentTime) / 1000 / 1000);
	}

	if ((buf->nRetry > session_param.Gp_interconnect_min_retries_before_timeout) &&
		(now - buf->sentTime) > ((uint64) session_param.Gp_interconnect_transmit_timeout * 1000 * 1000))
	{
		std::stringstream ss;
		ss <<"ERROR, interconnect encountered a network error, please check your network"<<
			"Failed to send packet (seq "<<buf->pkt->seq<<") to "<<buf->conn->remoteHostAndPort<<
			" (pid "<<buf->pkt->dstPid<<" cid "<<buf->pkt->dstContentId<<") after "<<buf->nRetry<<
			" retries in "<<session_param.Gp_interconnect_transmit_timeout<<" seconds.";
		throw ICNetworkException(ss.str(), __FILE__, __LINE__);
	}

	/*
	 * Default value of Gp_interconnect_transmit_timeout is one hours.
	 * It taks too long time to detect a network error and it is not user friendly.
	 *
	 * Packets would be dropped repeatly on some specific ports. We'd better have
	 * a warning messgage for this case and give the DBA a chance to detect this error
	 * earlier. Since packets would also be dropped when network is bad, we should not
	 * error out here, but just give a warning message. Erroring our is still handled
	 * by GUC Gp_interconnect_transmit_timeout as above. Note that warning message should
	 * be printed for each statement only once.
	 */
	if ((buf->nRetry >= session_param.Gp_interconnect_min_retries_before_timeout) && !(*networkTimeoutIsLogged))
	{
		LOG(WARNING, "interconnect may encountered a network error, please check your network"
					 "Failed to send packet (seq %d) to %s (pid %d cid %d) after %d retries.",
					 buf->pkt->seq, buf->conn->remoteHostAndPort, buf->pkt->dstPid, buf->pkt->dstContentId, buf->nRetry);
		*networkTimeoutIsLogged = true;
	}
}

/*
 * checkExpiration
 * 		Check whether packets expire. If a packet expires, resend the packet,
 * 		and adjust its position in the unack queue ring.
 *
 */
void
UDPConn::checkExpiration(ICChunkTransportState *transportStates, uint64 now)
{
	/* check for expiration */
	int	count = 0;
	int	retransmits = 0;
	UDPConn *currBuffConn = NULL;

	Assert(unack_queue_ring.currentTime != 0);

	if (unlikely(session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_TIMER_IC))
	{
		checkRtmTimeout(&mudp, now, 500, transportStates, this->entry_, this);
		return;
	}

	if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_ADVANCE_IC)
	{
		uint64 timer_span_time = unack_queue_ring.currentTime + TIMER_SPAN_LOSS;

		while (now >= (timer_span_time + unack_queue_ring.time_difference)  && count++ < UNACK_QUEUE_RING_SLOTS_NUM)
		{
			/* expired, need to resend them */
			ICBuffer   *curBuf = NULL;

			while ((curBuf = unack_queue_ring.slots[unack_queue_ring.idx].pop()) != NULL)
			{
				UDPConn *conn = static_cast<UDPConn*>(curBuf->conn);
				curBuf->nRetry++;

				/*
				 * Fixed Timeout Thresholds: Traditional TCP-style Retransmission Timeout
				 * (RTTVAR.RTO) calculations may be too rigid for networks with volatile
				 * latency. This leads to:
				 *   Premature Retransmissions: Unnecessary data resends during temporary
				 *     latency spikes, wasting bandwidth.
				 *   Delayed Recovery: Slow reaction to actual packet loss when RTO is
				 *     overly conservative.
				 * 
				 * Lack of Context Awareness: Static RTO ignores real-time network behavior
				 * patterns, reducing throughput and responsiveness.
				 * 
				 * Solution: Dynamic Timeout Threshold Adjustment
				 * Implements an adaptive timeout mechanism to optimize retransmission:
				 *   if (now < (curBuf->sentTime + conn->rttvar.rto)) {
				 *       uint32_t diff = (curBuf->sentTime + conn->rttvar.rto) - now;
				 *       // ... (statistical tracking and threshold adjustment)
				 *   }
				 *   Temporary Latency Spike: Uses max (conservative) to avoid false
				 *     retransmits, reducing bandwidth waste (vs. traditional mistaken
				 *     retransmissions).
				 *   Persistent Packet Loss: Prioritizes min (aggressive) via
				 *     weight_retrans, accelerating recovery (vs. slow fixed-RTO reaction).
				 *   Stable Network: Balances weights for equilibrium throughput (vs.
				 *     static RTO limitations).
				 */
				if (now < (curBuf->sentTime +  conn->rttvar.rto))
				{
#ifdef TIMEOUT_Z
					uint32_t diff = (curBuf->sentTime +  conn->rttvar.rto) - now;
					if(unack_queue_ring.retrans_count == 0 && unack_queue_ring.no_retrans_count == 0)
					{
						unack_queue_ring.min = diff;
						unack_queue_ring.max = diff;
					}

					if (diff < unack_queue_ring.min) unack_queue_ring.min = diff;
					if (diff > unack_queue_ring.max) unack_queue_ring.max = diff;

					if (unack_queue_ring.retrans_count == 0)
						unack_queue_ring.time_difference = unack_queue_ring.max;
					else if (unack_queue_ring.no_retrans_count == 0 && ic_statistics.retransmits < (session_param.Gp_interconnect_min_retries_before_timeout / 4)) 
						unack_queue_ring.time_difference = 0;
					else
					{
						uint32_t total_count = unack_queue_ring.retrans_count + unack_queue_ring.no_retrans_count;
						double weight_retrans = (double)unack_queue_ring.retrans_count / total_count;
						double weight_no_retrans = (double)unack_queue_ring.no_retrans_count / total_count;
						unack_queue_ring.time_difference = (uint32_t)(unack_queue_ring.max * weight_no_retrans + unack_queue_ring.min * weight_retrans);
					}
					
					++unack_queue_ring.no_retrans_count;
				}
				else
					++unack_queue_ring.retrans_count;
#endif

#ifdef TRANSFER_PROTOCOL_STATS
				trans_proto_stats.update(TPE_DATA_PKT_SEND, curBuf->pkt);
#endif

				currBuffConn = static_cast<UDPConn*>(curBuf->conn);
				putIntoUnackQueueRing(&unack_queue_ring,
									  curBuf,
									  currBuffConn->computeExpirationPeriod(curBuf->nRetry), getCurrentTime());
				struct icpkthdr *pkt_ = curBuf->pkt;

				pkt_->send_time = getCurrentTime();
				pkt_->recv_time = 0;
				pkt_->retry_times = curBuf->nRetry;

				currBuffConn->sendOnce(curBuf->pkt);

				/*
				 * Adaptive Retry Backoff with Polling for Network Asymmetry Mitigation
				 *
				 * This logic addresses two critical network pathologies:
				 *  1. RTO Distortion Amplification: 
				 *     - Packet loss in volatile networks causes RTO-based retransmission errors
				 *     - Multiple spurious retries increase network load and congestion collapse risk
				 *  2. Data Skew-Induced Starvation:
				 *     - Under unbalanced workloads, low-traffic nodes experience MON (Message Order Number) delays
				 *     - Delayed ACKs trigger false retransmissions even when packets arrive eventually
				 *     - Unacked queue inflation worsens congestion in high-traffic nodes
				 */
				int32_t loop_ack = curBuf->nRetry;
				uint32_t rto_min = UDP_RTO_MIN / 10;
				uint32_t rtoMs = conn->rttvar.rto / 1000;
				int32_t wait_time = rto_min > rtoMs ? rto_min : rtoMs;
				int32_t loop = 0;

				/*
				 * To optimize performance, we need to process all the time-out file descriptors (fds)
				 * in each batch together.
				 */
				if (loop_ack > 0)
				{
					while (loop++ < loop_ack)
					{
						if (this->entry_->pollAcks(wait_time))
						{
							this->entry_->handleAcks(false);
							curBuf->nRetry = 0;
							break;
						}

						struct icpkthdr *pkt_ = curBuf->pkt;
						pkt_->send_time = getCurrentTime();
						pkt_->recv_time = 0;
						pkt_->retry_times = curBuf->nRetry;
						currBuffConn->sendOnce(pkt_);

						if (loop_ack < (session_param.Gp_interconnect_min_retries_before_timeout / 10))
							wait_time += wait_time / 10;
						else if (loop_ack > (session_param.Gp_interconnect_min_retries_before_timeout / 10) && loop_ack < (session_param.Gp_interconnect_min_retries_before_timeout / 5))
							wait_time += RTO_MAX / 10;
						else if (loop_ack > (session_param.Gp_interconnect_min_retries_before_timeout / 5) && loop_ack < (session_param.Gp_interconnect_min_retries_before_timeout / 2))
							wait_time += RTO_MAX / 5;
						else if (loop_ack < (session_param.Gp_interconnect_min_retries_before_timeout))
							wait_time += RTO_MAX;
					};
				}

				if (loop_ack > session_param.Gp_interconnect_min_retries_before_timeout / 5)
					LOG(INFO, "Resending packet (seq %d) to %s (pid %d cid %d) with %d retries in %lu seconds",
							curBuf->pkt->seq, curBuf->conn->remoteHostAndPort,
							curBuf->pkt->dstPid, curBuf->pkt->dstContentId, curBuf->nRetry,
							(now - curBuf->sentTime) / 1000 / 1000);

				retransmits++;
				ic_statistics.retransmits++;
				currBuffConn->stat_count_resent++;
				currBuffConn->stat_max_resent = Max(currBuffConn->stat_max_resent,
													currBuffConn->stat_count_resent);

				UDPConn::checkNetworkTimeout(curBuf, now, &transportStates->networkTimeoutIsLogged);

#ifdef AMS_VERBOSE_LOGGING
				LOG(INFO, "RESEND pkt with seq %d (retry %d, rtt " UINT64_FORMAT ") to route %d",
						curBuf->pkt->seq, curBuf->nRetry, currBuffConn->rtt, currBuffConn->route);
				logPkt("RESEND PKT in checkExpiration", curBuf->pkt);
#endif
			}

			timer_span_time += TIMER_SPAN_LOSS;
			unack_queue_ring.idx = (unack_queue_ring.idx + 1) % (UNACK_QUEUE_RING_SLOTS_NUM);
		}
	}
	else
	{
		while (now >= (unack_queue_ring.currentTime + TIMER_SPAN) && count++ < UNACK_QUEUE_RING_SLOTS_NUM)
		{
			/* expired, need to resend them */
			ICBuffer   *curBuf = NULL;

			while ((curBuf = unack_queue_ring.slots[unack_queue_ring.idx].pop()) != NULL)
			{
				curBuf->nRetry++;
				currBuffConn = static_cast<UDPConn*>(curBuf->conn);
				putIntoUnackQueueRing(
									&unack_queue_ring,
									curBuf,
									currBuffConn->computeExpirationPeriod(curBuf->nRetry), now);

#ifdef TRANSFER_PROTOCOL_STATS
				trans_proto_stats.update(TPE_DATA_PKT_SEND, curBuf->pkt);
#endif

				currBuffConn->sendOnce(curBuf->pkt);

				retransmits++;
				ic_statistics.retransmits++;
				currBuffConn->stat_count_resent++;
				currBuffConn->stat_max_resent = Max(currBuffConn->stat_max_resent, currBuffConn->stat_count_resent);
				UDPConn::checkNetworkTimeout(curBuf, now, &transportStates->networkTimeoutIsLogged);

#ifdef AMS_VERBOSE_LOGGING
				LOG(INFO, "RESEND pkt with seq %d (retry %d, rtt " UINT64_FORMAT ") to route %d",
					  curBuf->pkt->seq, curBuf->nRetry, curBuf->conn->rtt, curBuf->conn->route);
				logPkt("RESEND PKT in checkExpiration", curBuf->pkt);
#endif
			}

			unack_queue_ring.currentTime += TIMER_SPAN;
			unack_queue_ring.idx = (unack_queue_ring.idx + 1) % (UNACK_QUEUE_RING_SLOTS_NUM);
		}

		/*
		 * deal with case when there is a long time this function is not called.
		 */
		unack_queue_ring.currentTime = now - (now % (TIMER_SPAN));
	}

	if (retransmits > 0)
	{
		snd_control_info.ssthresh = Max(snd_control_info.cwnd / 2, snd_control_info.minCwnd);
		snd_control_info.cwnd = snd_control_info.minCwnd;
	}
}

/*
 * checkDeadlock
 * 		Check whether deadlock occurs on a connection.
 *
 * What this function does is to send a status query message to rx thread when
 * the connection has not received any acks for some time. This is to avoid
 * potential deadlock when there are continuous ack losses. Packet resending
 * logic does not help avoiding deadlock here since the packets in the unack
 * queue may already been removed when the sender knows that they have been
 * already buffered in the receiver side queue.
 *
 * Some considerations on deadlock check time period:
 *
 * Potential deadlock occurs rarely. According to our experiments on various
 * workloads and hardware. It occurred only when fault injection is enabled
 * and a large number packets and acknowledgments are discarded. Thus, here we
 * use a relatively large deadlock check period.
 *
 */
void
UDPConn::checkDeadlock()
{
	uint64		deadlockCheckTime;

	if (this->unackQueue.length() == 0 && this->capacity == 0 && this->sndQueue.length() > 0)
	{
		/* we must have received some acks before deadlock occurs. */
		Assert(this->deadlockCheckBeginTime > 0);

#ifdef USE_ASSERT_CHECKING
		if (udp_testmode)
		{
			deadlockCheckTime = 100000;
		}
		else
#endif
		{
			deadlockCheckTime = DEADLOCK_CHECKING_TIME;
		}

		uint64		now = getCurrentTime();

		/* request the capacity to avoid the deadlock case */
		if (((now - ic_control_info.lastDeadlockCheckTime) > deadlockCheckTime) &&
			((now - this->deadlockCheckBeginTime) > deadlockCheckTime))
		{
			this->sendStatusQueryMessage(this->conn_info.seq - 1);
			ic_control_info.lastDeadlockCheckTime = now;
			ic_statistics.statusQueryMsgNum++;

			if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_ADVANCE_IC && this->entry_->pollAcks(50))
			{
				this->entry_->handleAcks(false);
				this->deadlockCheckBeginTime = now;
			}

			/* check network error. */
			if ((now - this->deadlockCheckBeginTime) > ((uint64) session_param.Gp_interconnect_transmit_timeout * 100 * 1000))
			{
				LOG(INFO, "Did not get any response from %s (pid %d cid %d) in 600 seconds.", this->remoteHostAndPort,
						  this->conn_info.dstPid, this->conn_info.dstContentId);

				if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_TIMER_IC)
					this->capacity += 1;

				if ((now - this->deadlockCheckBeginTime) > ((uint64)session_param.Gp_interconnect_transmit_timeout * 1000 * 1000))
				{
					std::stringstream ss;
					ss << "ERROR, interconnect encountered a network error, please check your network."
					   << "Did not get any response from " << remoteHostAndPort << " (pid " << conn_info.dstPid << " cid " << conn_info.dstContentId << ") in "
					   << session_param.Gp_interconnect_transmit_timeout << " seconds.";
					throw ICNetworkException(ss.str(), __FILE__, __LINE__);
				}
			}
		}
	}
}

/*
 * updateRetransmitStatistics
 * 		Update the retransmit statistics.
 */
void
UDPConn::updateRetransmitStatistics()
{
	ic_statistics.retransmits++;
	this->stat_count_resent++;
	this->stat_max_resent = Max(this->stat_max_resent, this->stat_count_resent);
}

/*
 * checkExpirationCapacityFC
 * 		Check expiration for capacity based flow control method.
 */
void
UDPConn::checkExpirationCapacityFC(int timeout)
{
	if (this->unackQueue.length() == 0)
		return;

	uint64		now = getCurrentTime();
	uint64		elapsed = now - ic_control_info.lastPacketSendTime;

	if (elapsed >= ((uint64) timeout * 1000))
	{
		ICBufferLink *bufLink = this->unackQueue.first();
		ICBuffer   *buf = GET_ICBUFFER_FROM_PRIMARY(bufLink);

		Assert(this == buf->conn);
		this->sendOnce(buf->pkt);

		buf->nRetry++;
		ic_control_info.lastPacketSendTime = now;

		this->updateRetransmitStatistics();
		UDPConn::checkNetworkTimeout(buf, now, &entry_->state->networkTimeoutIsLogged);
	}
}

/*
 * checkExceptions
 * 		Check exceptions including packet expiration, deadlock, bg thread error, NIC failure...
 * 		Caller should start from 0 with retry, so that the expensive check for deadlock and
 * 		QD connection can be avoided in a healthy state.
 */
void
UDPConn::checkExceptions(int retry, int timeout)
{
	if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_CAPACITY_IC
		/* || conn->state == mcsSetupOutgoingConnection */ )
	{
		this->checkExpirationCapacityFC(timeout);
	}

	if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_IC || session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_ADVANCE_IC)
	{
		uint64 now = getCurrentTime();

		if (now - ic_control_info.lastExpirationCheckTime > uint64(TIMER_CHECKING_PERIOD))
		{
			this->checkExpiration(this->entry_->state, now);
			ic_control_info.lastExpirationCheckTime = now;
		}
	}

	if ((retry & 0x3) == 2)
	{
		this->checkDeadlock();

		checkRxThreadError();
		CHECK_INTERRUPTS(this->entry_->state);
	}

	/*
	 * 1. NIC on master (and thus the QD connection) may become bad, check it.
	 * 2. Postmaster may become invalid, check it
	 *
	 * We check modulo 2 to correlate with the deadlock check above at the
	 * initial iteration.
	 */
	if ((retry & 0x3f) == 2)
	{
		checkQDConnectionAlive();
		CHECK_POSTMASTER_ALIVE();
	}
}

/*
 * computeTimeout
 * 		Compute timeout value in ms.
 */
int
UDPConn::computeTimeout(int retry)
{
	int32_t rtoMs = 0;

	rtoMs = this->rttvar.rto / 1000;
	if (this->unackQueue.length() == 0)
		return TIMER_CHECKING_PERIOD;

	ICBufferLink *bufLink = this->unackQueue.first();
	ICBuffer   *buf = GET_ICBUFFER_FROM_PRIMARY(bufLink);

	if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_ADVANCE_IC)
	{
		if (buf->nRetry == 0 && retry == 0 && unack_queue_ring.numSharedOutStanding < (snd_control_info.cwnd - snd_control_info.minCwnd))
			return 0;

		return rtoMs > TIMER_CHECKING_PERIOD ? rtoMs: TIMER_CHECKING_PERIOD;
	}

	if (buf->nRetry == 0 && retry == 0)
		return 0;

	if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_IC)
		return TIMER_CHECKING_PERIOD;

	/* for capacity based flow control */
	return TIMEOUT(buf->nRetry);
}

/*
 * UDPConn::Send
 * 		is used to send a tcItem to a single destination. Tuples often are
 * 		*very small* we aggregate in our local buffer before sending into the kernel.
 *
 * PARAMETERS
 *	 conn - UDPConn that the tcItem is to be sent to.
 *	 tcItem - message to be sent.
 *	 motionId - Node Motion Id.
 */
void
UDPConn::Send(DataBlock *data)
{
	int			length = data->len;
	int			retry = 0;
	bool		doCheckExpiration = false;
	bool		gotStops = false;

	Assert(this->msgSize > 0);

#ifdef AMS_VERBOSE_LOGGING
	LOG(DEBUG3, "UDPConn::Send(): msgSize %d this chunk length %d this seq %d",
			this->msgSize, data->len, this->conn_info.seq);
#endif

	if (this->msgSize + length <= global_param.Gp_max_packet_size)
	{
		memcpy(this->pBuff + this->msgSize, data->pos, data->len);
		this->msgSize += length;

		this->tupleCount++;
		return;
	}

	/* prepare this for transmit */
	ic_statistics.totalCapacity += this->capacity;
	ic_statistics.capacityCountingTime++;

	/* try to send it */
	this->prepareXmit();
	this->sndQueue.append(this->curBuff);
	this->sendBuffers();

	/* get a new buffer */
	this->curBuff = NULL;
	this->pBuff = NULL;

	uint64 now = getCurrentTime();

	if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_CAPACITY_IC)
		doCheckExpiration = false;
	else
		doCheckExpiration = (now - ic_control_info.lastExpirationCheckTime) > MAX_TIME_NO_TIMER_CHECKING ? true : false;

	ic_control_info.lastPacketSendTime = 0;
	this->deadlockCheckBeginTime = now;

	while (doCheckExpiration || (this->curBuff = snd_buffer_pool.get(this)) == NULL)
	{
		int timeout = (doCheckExpiration ? 0 : this->computeTimeout(retry));

		if (this->entry_->pollAcks(timeout))
		{
			if (this->entry_->handleAcks(true))
			{
				/*
				 * We make sure that we deal with the stop messages only after
				 * we get a buffer. Otherwise, if the stop message is not for
				 * this connection, this will lead to an error for the
				 * following data sending of this connection.
				 */
				gotStops = true;
			}
		}
		this->checkExceptions(retry++, timeout);
		doCheckExpiration = false;

		if (!doCheckExpiration && this->unackQueue.length() == 0 && this->capacity > 0 && this->sndQueue.length() > 0)
			this->sendBuffers();
	}

	this->pBuff = (uint8 *) this->curBuff->pkt;

	if (gotStops)
	{
		/* handling stop message will make some connection not active anymore */
		this->entry_->handleStopMsgs();

		if (!this->stillActive)
			return;
	}

	/* reinitialize connection */
	this->tupleCount = 0;
	this->msgSize = sizeof(this->conn_info);

	/* now we can copy the input to the new buffer */
	memcpy(this->pBuff + this->msgSize, data->pos, data->len);
	this->msgSize += length;

	this->tupleCount++;
}

/*
 * C++ implement for udp protocol.
 */
UDPConn::UDPConn(TransportEntry *entry)
{
	/* the field of MotionConn */
	this->sockfd        = -1;
	this->pBuff         = nullptr;
	this->msgSize       = 0;
	this->msgPos        = nullptr;
	this->recvBytes     = 0;
	this->tupleCount    = 0;
	this->stillActive   = false;
	this->stopRequested = false;
	this->cdbProc       = nullptr;
	this->remoteContentId      = -1;
	this->remoteHostAndPort[0] = '\0';
	this->opaque_data = nullptr;
	this->sent_record_typmod = 0;

	/* the field of UDPConn */
	this->capacity  = -1;
	this->sentSeq   = 0;
	this->receivedAckSeq = 0;
	this->consumedSeq    = 0;
	this->rtt = 0;
	this->dev = 0;
	this->deadlockCheckBeginTime = -1;
	this->curBuff  = nullptr;
	this->route    = 0;
	this->peer_len = 0;
	this->pkt_q_capacity = 0;
	this->pkt_q_size     = 0;
	this->pkt_q_head     = -1;
	this->pkt_q_tail     = -1;
	this->pkt_q          = nullptr;
	this->stat_total_ack_time = 0;
	this->stat_count_acks     = 0;
	this->stat_max_ack_time   = 0;
	this->stat_min_ack_time   = 0;
	this->stat_count_resent   = 0;
	this->stat_max_resent     = 0;
	this->stat_count_dropped  = 0;

	this->state = mcsNull;
	this->sockfd = -1;
	this->msgSize = 0;
	this->tupleCount = 0;
	this->stillActive = false;
	this->stopRequested = false;
	this->cdbProc = NULL;
	this->opaque_data = NULL;
	this->sent_record_typmod = 0;

	/*
	 * "UDPConn dummyconn(NULL)" will be called by handleMismatch() in rx thread,
	 * it will lead to the error: "palloc called from thread". So code below should
	 * be called in MakeSendEntry() and MakeRecvEntry();
	 * if (global_param.createOpaqueDataCallback)
	 *	this->opaque_data = global_param.createOpaqueDataCallback();
	 */

	this->entry_ = entry;
}

UDPConn*
TransportEntry::GetConn(int index)
{
	Assert(index >= 0);

	if (index >= 0 && static_cast<size_t>(index) < this->conns_.size())
		return this->conns_[index].get();

	std::stringstream ss;
	ss << "invalid index for conn, index: " << index << ", conn size: " << conns_.size();
	throw ICInvalidIndex(ss.str(), __FILE__, __LINE__);
}

/*
 * aggregateStatistics
 * 		aggregate statistics.
 */
void
TransportEntry::aggregateStatistics()
{
	/*
	 * We first clear the stats, and then compute new stats by aggregating the
	 * stats from each connection.
	 */
	this->stat_total_ack_time = 0;
	this->stat_count_acks = 0;
	this->stat_max_ack_time = 0;
	this->stat_min_ack_time = ~((uint64) 0);
	this->stat_count_resent = 0;
	this->stat_max_resent = 0;
	this->stat_count_dropped = 0;

	Assert(this->numConns == static_cast<int>(this->conns_.size()));
	for (int connNo = 0; connNo < this->numConns; connNo++)
	{
		UDPConn *conn = this->GetConn(connNo);

		this->stat_total_ack_time += conn->stat_total_ack_time;
		this->stat_count_acks += conn->stat_count_acks;
		this->stat_max_ack_time = Max(this->stat_max_ack_time, conn->stat_max_ack_time);
		this->stat_min_ack_time = Min(this->stat_min_ack_time, conn->stat_min_ack_time);
		this->stat_count_resent += conn->stat_count_resent;
		this->stat_max_resent = Max(this->stat_max_resent, conn->stat_max_resent);
		this->stat_count_dropped += conn->stat_count_dropped;
	}
}

/*
 * handleAck
 * 		handle acks incoming from our upstream peers.
 *
 * if we receive a stop message, return true (caller will clean up).
 */
bool
TransportEntry::handleAcks(bool need_flush)
{
	bool		ret = false;
	UDPConn *ackConn = NULL;
	int			n;

	struct sockaddr_storage peer;
	socklen_t	peerlen;

	struct icpkthdr *pkt = snd_control_info.ackBuffer;
	bool shouldSendBuffers = false;

	for (;;)
	{

		/* ready to read on our socket ? */
		peerlen = sizeof(peer);
		n = recvfrom(this->txfd, (char *) pkt, MIN_PACKET_SIZE, 0,
					 (struct sockaddr *) &peer, &peerlen);

		if (n < 0)
		{
			if (errno == EWOULDBLOCK)	/* had nothing to read. */
			{
				this->aggregateStatistics();
				return ret;
			}

			CHECK_INTERRUPTS(this->state);

			if (errno == EINTR)
				continue;

			throw ICNetworkException("ERROR, interconnect error waiting for peer ack, During recvfrom() call.", __FILE__, __LINE__);
		}
		else if (n < int(sizeof(struct icpkthdr)))
		{
			continue;
		}
		else if (n != int(pkt->len))
		{
			continue;
		}

		/*
		 * check the CRC of the payload.
		 */
		if (session_param.gp_interconnect_full_crc)
		{
			if (!checkCRC(pkt))
			{
				ic_atomic_add_fetch_u32((ic_atomic_uint32 *) &ic_statistics.crcErrors, 1);
				if (IC_DEBUG2 >= session_param.log_min_messages)
					LOG(DEBUG2, "received network data error, dropping bad packet, user data unaffected.");
				continue;
			}
		}

		/*
		 * read packet, is this the ack we want ?
		 */
		if (pkt->srcContentId == global_param.segindex &&
			pkt->srcPid == global_param.MyProcPid &&
			pkt->srcListenerPort == (UDP2_GetListenPortUDP()) &&
			pkt->sessionId == session_param.gp_session_id &&
			pkt->icId == this->state->icInstanceId)
		{
			Assert(pkt->motNodeId == motNodeId);
			LOG(DEBUG3, "TransportEntry::handleAcks(): icid: %d, motNodeId: %d, srcSeg: %d, dstSeg: %d, srcPid: %d, dstPid: %d, seq: %d, extraSeq: %d, len: %d, flags: %s",
					pkt->icId, pkt->motNodeId, pkt->srcContentId, pkt->dstContentId, pkt->srcPid, pkt->dstPid, pkt->seq, pkt->extraSeq, pkt->len, flags2txt(pkt->flags));

			/*
			 * packet is for me. Note here we do not need to get a connection
			 * lock here, since background rx thread only read the hash table.
			 */
			ackConn = ic_control_info.connHtab.find(pkt);
			if (ackConn == NULL)
			{
				LOG(INFO, "Received ack for unknown connection (flags 0x%x)", pkt->flags);
				continue;
			}

			ackConn->stat_count_acks++;
			ic_statistics.recvAckNum++;

			uint64 now = getCurrentTime();

			ackConn->deadlockCheckBeginTime = now;

			/*
			 * We simply disregard pkt losses (NAK) due to process start race
			 * (that is, sender is started earlier than receiver. rx
			 * background thread may receive packets when connections are not
			 * created yet).
			 *
			 * Another option is to resend the packet immediately, but
			 * experiments do not show any benefits.
			 */
			if (pkt->flags & UDPIC_FLAGS_NAK)
				continue;

			while (true)
			{
				if (pkt->flags & UDPIC_FLAGS_CAPACITY)
				{
					if (pkt->extraSeq > ackConn->consumedSeq)
					{
						ackConn->capacity += pkt->extraSeq - ackConn->consumedSeq;
						ackConn->consumedSeq = pkt->extraSeq;
						shouldSendBuffers = true;
					}
				}
				else if (pkt->flags & UDPIC_FLAGS_DUPLICATE)
				{
					if (IC_DEBUG1 >= session_param.log_min_messages)
						LOG(DEBUG1, "GOTDUPACK [seq %d] from route %d; srcpid %d dstpid %d cmd %d flags 0x%x connseq %d",
								  pkt->seq, ackConn->route, pkt->srcPid, pkt->dstPid, pkt->icId, pkt->flags, ackConn->conn_info.seq);

					shouldSendBuffers |= (ackConn->handleAckForDuplicatePkt(pkt));
					break;
				}
				else if (pkt->flags & UDPIC_FLAGS_DISORDER)
				{
					if (IC_DEBUG1 >= session_param.log_min_messages)
						LOG(DEBUG1, "GOTDISORDER [seq %d] from route %d; srcpid %d dstpid %d cmd %d flags 0x%x connseq %d",
									pkt->seq, ackConn->route, pkt->srcPid, pkt->dstPid, pkt->icId, pkt->flags, ackConn->conn_info.seq);

					shouldSendBuffers |= (ackConn->handleAckForDisorderPkt(pkt));
					break;
				}
				else if (pkt->flags & UDPIC_FLAGS_FULL)
				{
					if (IC_DEBUG1 >= session_param.log_min_messages)
						LOG(DEBUG1, "Recv buff is full [seq %d] from route %d; srcpid %d dstpid %d cmd %d flags 0x%x connseq %d", pkt->seq, ackConn->route, pkt->srcPid, pkt->dstPid, pkt->icId, pkt->flags, ackConn->conn_info.seq);
					break;
				}

				/*
				 * don't get out of the loop if pkt->seq equals to
				 * ackConn->receivedAckSeq, need to check UDPIC_FLAGS_STOP
				 * flag
				 */
				if (pkt->seq < ackConn->receivedAckSeq)
				{
					if (IC_DEBUG1 >= session_param.log_min_messages)
						LOG(DEBUG1, "ack with bad seq?! expected (%d, %d] got %d flags 0x%x, capacity %d consumedSeq %d",
									ackConn->receivedAckSeq, ackConn->sentSeq, pkt->seq, pkt->flags, ackConn->capacity, ackConn->consumedSeq);
					break;
				}

				/* haven't gotten a stop request, maybe this is one ? */
				if ((pkt->flags & UDPIC_FLAGS_STOP) && !ackConn->stopRequested && ackConn->stillActive)
				{
#ifdef AMS_VERBOSE_LOGGING
					LOG(INFO, "got ack with stop; srcpid %d dstpid %d cmd %d flags 0x%x pktseq %d connseq %d",
							  pkt->srcPid, pkt->dstPid, pkt->icId, pkt->flags, pkt->seq, ackConn->conn_info.seq);
#endif
					ackConn->stopRequested = true;
					ackConn->conn_info.flags |= UDPIC_FLAGS_STOP;
					ret = true;
					/* continue to deal with acks */
				}

				if (pkt->seq == ackConn->receivedAckSeq)
				{
					if (IC_DEBUG1 >= session_param.log_min_messages)
						LOG(DEBUG1, "ack with bad seq?! expected (%d, %d] got %d flags 0x%x, capacity %d consumedSeq %d",
									ackConn->receivedAckSeq, ackConn->sentSeq, pkt->seq, pkt->flags, ackConn->capacity, ackConn->consumedSeq);
					break;
				}

				/* deal with a regular ack. */
				if (pkt->flags & UDPIC_FLAGS_ACK)
				{
					ICBufferLink *link = NULL;
					ICBufferLink *next = NULL;
					ICBuffer   *buf = NULL;

#ifdef AMS_VERBOSE_LOGGING
					LOG(INFO, "GOTACK [seq %d] from route %d; srcpid %d dstpid %d cmd %d flags 0x%x connseq %d",
							  pkt->seq, ackConn->route, pkt->srcPid, pkt->dstPid, pkt->icId, pkt->flags, ackConn->conn_info.seq);
#endif

					link = ackConn->unackQueue.first();
					buf = GET_ICBUFFER_FROM_PRIMARY(link);

					while (!ackConn->unackQueue.is_head(link) && buf->pkt->seq <= pkt->seq)
					{
						next = link->next;
						ackConn->handleAckedPacket(buf, now, pkt);
						shouldSendBuffers = true;
						link = next;
						buf = GET_ICBUFFER_FROM_PRIMARY(link);
					}
				}
				break;
			}

			/*
			 * When there is a capacity increase or some outstanding buffers
			 * removed from the unack queue ring, we should try to send
			 * buffers for the connection. Even when stop is received, we
			 * still send here, since in STOP/EOS race case, we may have been
			 * in EOS sending logic and will not check stop message.
			 */
			if (shouldSendBuffers && need_flush)
				ackConn->sendBuffers();
		}
		else
		{
			if (IC_DEBUG1 >= session_param.log_min_messages)
				LOG(DEBUG1, "handleAck: not the ack we're looking for (flags 0x%x)...mot(%d) content(%d:%d) srcpid(%d:%d) "
						"dstpid(%d) srcport(%d:%d) dstport(%d) sess(%d:%d) cmd(%d:%d)",
						pkt->flags, pkt->motNodeId, pkt->srcContentId, global_param.segindex,
						pkt->srcPid, global_param.MyProcPid, pkt->dstPid, pkt->srcListenerPort,
						(UDP2_GetListenPortUDP()), pkt->dstListenerPort, pkt->sessionId, session_param.gp_session_id,
						pkt->icId, this->state->icInstanceId);
		}
	}

	return ret;
}

/*
 * handleStopMsgs
 *		handle stop messages.
 *
 */
void
TransportEntry::handleStopMsgs()
{
	int			i = 0;

#ifdef AMS_VERBOSE_LOGGING
	LOG(DEBUG3, "handleStopMsgs: node %d", this->motNodeId);
#endif

	while (i < this->numConns)
	{
		UDPConn *conn = this->GetConn(i);

#ifdef AMS_VERBOSE_LOGGING
		LOG(DEBUG3, "handleStopMsgs: node %d route %d %s %s", this->motNodeId, conn->route,
				(conn->stillActive ? "active" : "NOT active"), (conn->stopRequested ? "stop requested" : ""));
		LOG(DEBUG3, "handleStopMsgs: node %d route %d msgSize %d",
				this->motNodeId, conn->route, conn->msgSize);
#endif

		/*
		 * MPP-2427: we're guaranteed to have recently flushed, but this might
		 * not be empty (if we got a stop on a buffer that wasn't the one we
		 * were sending) ... empty it first so the outbound buffer is empty
		 * when we get here.
		 */
		conn->handleStop();

		i++;

		if (i == this->numConns)
		{
			if (this->pollAcks(0))
			{
				bool rs = this->handleAcks(true);
				if (rs)
				{
					/* more stops found, loop again. */
					i = 0;
					continue;
				}
			}
		}
	}
}

/*
 * pollAcks
 * 		Timeout polling of acks
 */
bool
TransportEntry::pollAcks(int timeout)
{
	struct pollfd nfd;
	int			n;

	nfd.fd = this->txfd;
	nfd.events = POLLIN;

	n = poll(&nfd, 1, timeout);
	if (n < 0)
	{
		CHECK_INTERRUPTS(this->state);

		if (errno == EINTR)
			return false;

		throw ICNetworkException("ERROR, interconnect error waiting for peer ack During poll() call.", __FILE__, __LINE__);
		/* not reached */
	}

	if (n == 0)					/* timeout */
	{
		return false;
	}

	/* got an ack to handle (possibly a stop message) */
	if (n == 1 && (nfd.events & POLLIN))
	{
		return true;
	}

	return false;
}

std::unique_ptr<TransportEntry>
TransportEntry::MakeRecvEntry(CChunkTransportStateImpl *state,
							  int icid,
							  ICExecSlice *sendSlice,
							  ICExecSlice *recvSlice)
{
	int			incoming_count = 0;
	int			expectedTotalIncoming = 0;

	Assert(sendSlice->sliceIndex > 0);
	Assert(recvSlice->sliceIndex >= 0);

	int motNodeID = sendSlice->sliceIndex;
	int numConns  = sendSlice->numPrimaryProcesses;

	std::unique_ptr<TransportEntry> pEntry =
		std::make_unique<TransportEntry>(state, motNodeID, numConns, sendSlice, recvSlice);

	pEntry->conns_.resize(numConns);
	for (int i = 0; i < numConns; ++i)
	{
		pEntry->conns_[i] = std::make_unique<UDPConn>(pEntry.get());
		UDPConn *conn = pEntry->conns_[i].get();

		Assert(i < sendSlice->numPrimaryProcesses);
		ICCdbProcess *cdbProc = sendSlice->primaryProcesses + i;
		if (cdbProc->valid)
		{
			conn->cdbProc = cdbProc;

			expectedTotalIncoming++;

			/* rx_buffer_queue */
			conn->pkt_q_capacity = session_param.Gp_interconnect_queue_depth;
			conn->pkt_q_size = 0;
			conn->pkt_q_head = 0;
			conn->pkt_q_tail = 0;

			if (global_param.simpleFaultInjectorCallback)
				global_param.simpleFaultInjectorCallback("interconnect_setup_palloc");

			conn->pkt_q = (uint8 **) ic_malloc0(conn->pkt_q_capacity * sizeof(uint8 *));

			/* update the max buffer count of our rx buffer pool.  */
			rx_buffer_pool.maxCount += conn->pkt_q_capacity;

			/*
			 * connection header info (defining characteristics of this
			 * connection)
			 */
			memset(&conn->conn_info, 0, sizeof(conn->conn_info));
			conn->route = i;

			conn->conn_info.seq = 1;
			conn->stillActive = true;

			incoming_count++;

			conn->conn_info.motNodeId = pEntry->motNodeId;
			conn->conn_info.recvSliceIndex = recvSlice->sliceIndex;
			conn->conn_info.sendSliceIndex = sendSlice->sliceIndex;

			conn->conn_info.srcContentId = conn->cdbProc->contentid;
			conn->conn_info.dstContentId = global_param.segindex;

			conn->conn_info.srcListenerPort = conn->cdbProc->listenerPort;
			conn->conn_info.dstListenerPort = UDP2_GetListenPortUDP();
			conn->conn_info.srcPid          = conn->cdbProc->pid;
			conn->conn_info.dstPid          = global_param.MyProcPid;
			conn->conn_info.sessionId       = session_param.gp_session_id;
			conn->conn_info.icId            = icid;
			conn->conn_info.flags           = UDPIC_FLAGS_RECEIVER_TO_SENDER;

			conn->rttvar.ts_rto = 0;
			conn->rttvar.rto = UDP_INITIAL_RTO;
			conn->rttvar.srtt = 0;
			conn->rttvar.rttvar = 0;
			conn->rttvar.snd_una = 0;
			conn->rttvar.nrtx = 0;
			conn->rttvar.max_nrtx = 0;
			conn->rttvar.mss = UDP_DEFAULT_MSS;
			conn->rttvar.cwnd = 2;
			conn->rttvar.ssthresh = UDP_INFINITE_SSTHRESH;
			conn->rttvar.loss_count = 0;
			conn->rttvar.karn_mode = false;
			conn->on_rto_idx = -1;
			ic_control_info.connHtab.add(conn);

			if (global_param.createOpaqueDataCallback)
				conn->opaque_data = global_param.createOpaqueDataCallback();
		}
	}

	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
	{
		LOG(DEBUG1, "SetupUDPInterconnect will activate "
					"%d incoming, %d expect incoming for ic_instancce_id %d.",
					incoming_count, expectedTotalIncoming, icid);
	}

	return pEntry;
}

std::unique_ptr<TransportEntry>
TransportEntry::MakeSendEntry(CChunkTransportStateImpl *state,
							  int icid,
							  ICExecSlice *sendSlice,
							  ICExecSlice *recvSlice)
{
	int			outgoing_count = 0;
	int			expectedTotalOutgoing = 0;

	Assert(sendSlice->sliceIndex > 0);
	Assert(recvSlice->sliceIndex >= 0);

	int motNodeID = sendSlice->sliceIndex;
	int numConns  = recvSlice->numPrimaryProcesses;

	std::unique_ptr<TransportEntry> pEntry =
		std::make_unique<TransportEntry>(state, motNodeID, numConns, sendSlice, recvSlice);

	pEntry->txfd = ICSenderSocket;
	pEntry->txport = ICSenderPort;
	pEntry->txfd_family = ICSenderFamily;

	int route = 0;
	pEntry->conns_.resize(numConns);

	for (int i = 0; i < numConns; ++i)
	{
		pEntry->conns_[i] = std::make_unique<UDPConn>(pEntry.get());
		UDPConn *conn = pEntry->conns_[i].get();

		/*
		* Setup a MotionConn entry for each of our outbound connections. Request
		* a connection to each receiving backend's listening port. NB: Some
		* mirrors could be down & have no CdbProcess entry.
		*/
		ICCdbProcess *cdbProc = recvSlice->primaryProcesses + i;
		if (cdbProc->valid)
		{
			conn->cdbProc = cdbProc;
			conn->sndQueue.init(ICBufferListType_Primary);
			conn->unackQueue.init(ICBufferListType_Primary);
			conn->capacity = session_param.Gp_interconnect_queue_depth;

			/* send buffer pool must be initialized before this. */
			snd_buffer_pool.maxCount += session_param.Gp_interconnect_snd_queue_depth;
			snd_control_info.cwnd += 1;
			conn->curBuff = snd_buffer_pool.get(conn);

			/* should have at least one buffer for each connection */
			Assert(conn->curBuff != NULL);

			conn->rtt = DEFAULT_RTT;
			conn->dev = DEFAULT_DEV;
			conn->deadlockCheckBeginTime = 0;
			conn->tupleCount = 0;
			conn->msgSize = sizeof(conn->conn_info);
			conn->sentSeq = 0;
			conn->receivedAckSeq = 0;
			conn->consumedSeq = 0;
			conn->pBuff = (uint8 *) conn->curBuff->pkt;
			conn->state = mcsSetupOutgoingConnection;
			conn->route = route++;
			expectedTotalOutgoing++;

			setupOutgoingUDPConnection(icid, pEntry.get(), conn);
			outgoing_count++;

			if (global_param.createOpaqueDataCallback)
				conn->opaque_data = global_param.createOpaqueDataCallback();
		}
	}

	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
	{
		LOG(DEBUG1, "SetupUDPInterconnect will activate "
					"%d outgoing, %d expect outgoing routes for ic_instancce_id %d.",
					outgoing_count, expectedTotalOutgoing, icid);
	}

	return pEntry;
}

TransportEntry::TransportEntry(CChunkTransportStateImpl *state,
							   int motNodeID,
							   int numConns,
							   ICExecSlice *sendSlice,
							   ICExecSlice *recvSlice)
{
	/* the field of CChunkTransportStateEntry */
	this->valid     = false;
	this->conns     = nullptr;

	/* the field of TransportEntry */
	this->txfd        = -1;
	this->txfd_family = -1;
	this->txport      = 0;
	this->sendingEos  = false;
	this->stat_total_ack_time = 0;
	this->stat_count_acks     = 0;
	this->stat_max_ack_time   = 0;
	this->stat_min_ack_time   = 0;
	this->stat_count_resent   = 0;
	this->stat_max_resent     = 0;
	this->stat_count_dropped  = 0;

	this->motNodeId = motNodeID;
	this->numConns  = numConns;
	this->scanStart = 0;
	this->sendSlice = sendSlice;
	this->recvSlice = recvSlice;
	this->state     = state;
	this->valid     = true;
}

/*
 * receiveChunksUDPIFC
 * 		Receive chunks from the senders
 *
 * MUST BE CALLED WITH ic_control_info.lock LOCKED.
 */
void
TransportEntry::receiveChunksUDPIFC(int16 *srcRoute,
									UDPConn *conn,
									GetDataLenInPacket getLen,
									DataBlock *data)
{
	int			retries = 0;
	bool		directed = false;

#ifdef AMS_VERBOSE_LOGGING
	LOG(DEBUG5, "receivechunksUDP: motnodeid %d", this->motNodeId);
#endif

	if (conn != nullptr)
	{
		directed = true;
		*srcRoute = conn->route;
		rx_control_info.mainWaitingState.set(this->motNodeId, conn->route, this->state->icInstanceId);
	}
	else
	{
		/* non-directed receive */
		rx_control_info.mainWaitingState.set(this->motNodeId, ANY_ROUTE, this->state->icInstanceId);
	}

	std::unique_lock<std::mutex> lock(mtx);
	auto timeout = std::chrono::milliseconds(MAIN_THREAD_COND_TIMEOUT_MS);

	/* we didn't have any data, so we've got to read it from the network. */
	for (;;)
	{
		UDPConn *rxconn = nullptr;

		/* 1. Do we have data ready */
		if (rx_control_info.mainWaitingState.reachRoute != ANY_ROUTE)
		{
			rxconn = this->GetConn(rx_control_info.mainWaitingState.reachRoute);
			rxconn->prepareRxConnForRead();

			LOG(DEBUG2, "receiveChunksUDPIFC: non-directed rx woke on route %d", rx_control_info.mainWaitingState.reachRoute);
			rx_control_info.mainWaitingState.reset();
		}

		this->aggregateStatistics();
		if (rxconn)
		{
			Assert(rxconn->pBuff);

			pthread_mutex_unlock(&ic_control_info.lock);

			LOG(DEBUG2, "got data with length %d", rxconn->recvBytes);
			/* successfully read into this connection's buffer. */
			rxconn->GetDataInBuf(getLen, data);

			if (!directed)
				*srcRoute = rxconn->route;

			return;
		}

		retries++;

		/*
		 * Ok, we've processed all the items currently in the queue. Arm the
		 * latch (before releasing the mutex), and wait for more messages to
		 * arrive. The RX thread will wake us up using the latch.
		 */
		pthread_mutex_unlock(&ic_control_info.lock);

		if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
		{
			LOG(DEBUG5, "waiting (timed) on route %d %s", rx_control_info.mainWaitingState.waitingRoute,
						(rx_control_info.mainWaitingState.waitingRoute == ANY_ROUTE ? "(any route)" : ""));
		}

		/*
		 * Wait for data to become ready.
		 *
		 * In the QD, also wake up immediately if any QE reports an
		 * error through the main QD-QE libpq connection. For that, ask
		 * the dispatcher for a file descriptor to wait on for that.
		 */
		cv.wait_for(lock, timeout, []{return rx_control_info.mainWaitingState.reachRoute != ANY_ROUTE;});

		/* check the potential errors in rx thread. */
		checkRxThreadError();

		/* do not check interrupts when holding the lock */
		CHECK_INTERRUPTS(this->state);

		/* check to see if the task coordinator should cancel */
		CHECK_CANCEL(this->state);

		/*
		 * 1. NIC on master (and thus the QD connection) may become bad, check
		 * it. 2. Postmaster may become invalid, check it
		 */
		if ((retries & 0x3f) == 0)
		{
			checkQDConnectionAlive();
			CHECK_POSTMASTER_ALIVE();
		}

		pthread_mutex_lock(&ic_control_info.lock);

	} /* for (;;) */

	/* We either got data, or get cancelled. We never make it out to here. */
	return;				/* make GCC behave */
}

void
UDPConn::GetDataInBuf(GetDataLenInPacket getLen, DataBlock *data)
{
	int bytesProcessed = 0;

	Assert(data);

#ifdef AMS_VERBOSE_LOGGING
	LOG(DEBUG5, "recvtuple chunk recv bytes %d msgsize %d conn->pBuff %p conn->msgPos: %p",
			this->recvBytes, this->msgSize, this->pBuff, this->msgPos);
#endif

	int ic_hdr_size = sizeof(struct icpkthdr);

	data->pos = this->msgPos + ic_hdr_size;
	int rc = getLen ? getLen(data->pos, this->msgSize - ic_hdr_size) : (this->msgSize - ic_hdr_size);
	if (rc < 0)
	{
		std::stringstream ss;
		ss << "Failed to call getLen in GetDataInBuf, result: " << rc;
		throw ICException(ss.str(), __FILE__, __LINE__);
	}
	data->len = rc;

	bytesProcessed += ic_hdr_size;
	bytesProcessed += data->len;

	Assert(bytesProcessed == this->msgSize);

	this->recvBytes -= this->msgSize;
	if (this->recvBytes != 0)
	{
#ifdef AMS_VERBOSE_LOGGING
		LOG(DEBUG5, "residual message %d bytes", this->recvBytes);
#endif
		this->msgPos += this->msgSize;
	}

	this->msgSize = 0;
}

/*
 * RecvTupleChunkFromAnyUDPIFC_Internal
 * 		Receive tuple chunks from any route (connections)
 */
void
TransportEntry::RecvAny(int16 *srcRoute,
						GetDataLenInPacket getLen,
						DataBlock *data)
{
	int			index,
				activeCount = 0;
	bool		found = false;

	UDPConn *conn;

	index = this->scanStart;

	pthread_mutex_lock(&ic_control_info.lock);

	for (int i = 0; i < this->numConns; i++, index++)
	{
		if (index >= this->numConns)
			index = 0;

		conn = this->GetConn(index);
		if (conn->stillActive)
			activeCount++;

		ic_statistics.totalRecvQueueSize += conn->pkt_q_size;
		ic_statistics.recvQueueSizeCountingTime++;

		if (conn->pkt_q_size > 0)
		{
			found = true;
			conn->prepareRxConnForRead();
			break;
		}
	}

	if (found)
	{
		pthread_mutex_unlock(&ic_control_info.lock);

		conn->GetDataInBuf(getLen, data);

		*srcRoute = conn->route;
		this->scanStart = index + 1;
		return;
	}

	/* no data pending in our queue */

#ifdef AMS_VERBOSE_LOGGING
	LOG(INFO, "RecvTupleChunkFromAnyUDPIFC(): activeCount is %d", activeCount);
#endif
	if (activeCount == 0)
	{
		pthread_mutex_unlock(&ic_control_info.lock);
		return;
	}

	/* receiveChunksUDPIFC() releases ic_control_info.lock as a side-effect */
	this->receiveChunksUDPIFC(srcRoute, nullptr, getLen, data);

	this->scanStart = *srcRoute + 1;
}

/*
 * RecvTupleChunkFromUDPIFC_Internal
 * 		Receive tuple chunks from a specific route (connection)
 */
void
TransportEntry::RecvRoute(int16 srcRoute, GetDataLenInPacket getLen, DataBlock *data)
{
	UDPConn *conn = this->GetConn(srcRoute);

#ifdef AMS_VERBOSE_LOGGING
	if (!conn->stillActive)
	{
		LOG(INFO, "RecvTupleChunkFromUDPIFC(): connection inactive ?!");
	}
#endif

	pthread_mutex_lock(&ic_control_info.lock);

	if (!conn->stillActive)
	{
		pthread_mutex_unlock(&ic_control_info.lock);
		return;
	}

	ic_statistics.totalRecvQueueSize += conn->pkt_q_size;
	ic_statistics.recvQueueSizeCountingTime++;

	if (conn->pkt_q[conn->pkt_q_head] != NULL)
	{
		conn->prepareRxConnForRead();

		pthread_mutex_unlock(&ic_control_info.lock);

		conn->GetDataInBuf(getLen, data);

		return;
	}

	/* no existing data, we've got to read a packet */
	/* receiveChunksUDPIFC() releases ic_control_info.lock as a side-effect */
	int16 route;
	this->receiveChunksUDPIFC(&route, conn, getLen, data);
}

/*
 * TeardownUDPIFCInterconnect_Internal
 * 		Helper function for TeardownUDPIFCInterconnect.
 *
 * Developers should pay attention to:
 *
 * 1) Do not handle interrupts/throw errors in Teardown, otherwise, Teardown may be called twice.
 *    It will introduce an undefined behavior. And memory leaks will be introduced.
 *
 * 2) Be careful about adding elog/ereport/write_log in Teardown function,
 *    esp, out of HOLD_INTERRUPTS/RESUME_INTERRUPTS pair, since elog/ereport/write_log may
 *    handle interrupts.
 *
 */
void
CChunkTransportStateImpl::teardown(bool hasErrors)
{
	bool		isReceiver = false;

	/* Log the start of TeardownInterconnect. */
	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_TERSE_IC)
	{
		LogSeverity elevel = INFO;

		if (hasErrors || !this->activated)
		{
			if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
				elevel = INFO;
			else
				elevel = DEBUG1;
		}
		else if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
			elevel = DEBUG4;

		if (elevel)
		{
			ICExecSlice *mySlice = &this->sliceTable->slices[this->sliceId];
			LOG(elevel, "Interconnect seg%d slice%d cleanup state: %s; setup was %s",
						global_param.segindex, mySlice->sliceIndex,
						hasErrors ? "hasErrors" : "normal",
						this->activated ? "completed" : "exited");
		}

		/* if setup did not complete, log the slicetable */
		if (!this->activated && session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
		{
			//elog_node_display(DEBUG3, "local slice table", this->sliceTable, true);
			//TODO: print real slicetable;
			LOG(DEBUG3, "local slice table: ####");
		}
	}

	/*
	 * The long jump with CheckXXX() in receiveChunksUDPIFC() introduces the
	 * locked mtx, so unlock it here.
	 */
	mtx.unlock();

	/*
	 * add lock to protect the hash table, since background thread is still
	 * working.
	 */
	pthread_mutex_lock(&ic_control_info.lock);

	if (session_param.gp_interconnect_cache_future_packets)
		cleanupStartupCache();

	/*
	 * Now "normal" connections which made it through our peer-registration
	 * step. With these we have to worry about "in-flight" data.
	 */
	this->DestroySendEntries();

	/*
	 * Previously, there is a piece of code that deals with pending stops. Now
	 * it is delegated to background rx thread which will deal with any
	 * mismatched packets.
	 */

	/*
	 * cleanup all of our Receiving Motion nodes, these get closed immediately
	 * (the receiver know for real if they want to shut down -- they aren't
	 * going to be processing any more data).
	 */
	this->DestroyRecvEntries(&isReceiver);

	/*
	 * now that we've moved active rx-buffers to the freelist, we can prune
	 * the freelist itself
	 */
	while (rx_buffer_pool.count > rx_buffer_pool.maxCount)
	{
		icpkthdr   *buf = NULL;

		/* If this happened, there are some memory leaks.. */
		if (rx_buffer_pool.freeList == NULL)
		{
			pthread_mutex_unlock(&ic_control_info.lock);

			std::stringstream ss;
			ss << "FATAL: freelist NULL: count " << rx_buffer_pool.count
			   << " max " << rx_buffer_pool.maxCount << " buf " << rx_buffer_pool.freeList;
			throw ICFatalException(ss.str(), __FILE__, __LINE__);
		}

		buf = rx_buffer_pool.get_free();
		rx_buffer_pool.release(buf);
	}

	/*
	 * Update the history of interconnect instance id.
	 */
	if (global_param.Gp_role == GP_ROLE_DISPATCH_IC)
	{
		rx_control_info.cursorHistoryTable.update(this->icInstanceId, 0);
	}
	else if (global_param.Gp_role == GP_ROLE_EXECUTE_IC)
	{
		rx_control_info.lastTornIcId = this->icInstanceId;
	}

	if (IC_DEBUG1 >= session_param.log_min_messages)
	{
		LOG(DEBUG1, "Interconnect State: "
					"isSender %d isReceiver %d "
					"snd_queue_depth %d recv_queue_depth %d Gp_max_packet_size %d "
					"UNACK_QUEUE_RING_SLOTS_NUM %d TIMER_SPAN %lld DEFAULT_RTT %d "
					"hasErrors %d, ic_instance_id %d ic_id_last_teardown %d "
					"snd_buffer_pool.count %d snd_buffer_pool.maxCount %d snd_sock_bufsize %d recv_sock_bufsize %d "
					"snd_pkt_count %d retransmits %d crc_errors %d"
					" recv_pkt_count %d recv_ack_num %d"
					" recv_queue_size_avg %f"
					" capacity_avg %f"
					" freebuf_avg %f "
					"mismatch_pkt_num %d disordered_pkt_num %d duplicated_pkt_num %d"
					" cwnd %f status_query_msg_num %d",
					ic_control_info.isSender, isReceiver,
					session_param.Gp_interconnect_snd_queue_depth, session_param.Gp_interconnect_queue_depth, global_param.Gp_max_packet_size,
					UNACK_QUEUE_RING_SLOTS_NUM, TIMER_SPAN, DEFAULT_RTT,
					hasErrors, this->icInstanceId, rx_control_info.lastTornIcId,
					snd_buffer_pool.count, snd_buffer_pool.maxCount, ic_control_info.socketSendBufferSize, ic_control_info.socketRecvBufferSize,
					ic_statistics.sndPktNum, ic_statistics.retransmits, ic_statistics.crcErrors,
					ic_statistics.recvPktNum, ic_statistics.recvAckNum,
					(double) ((double) ic_statistics.totalRecvQueueSize) / ((double) ic_statistics.recvQueueSizeCountingTime),
					(double) ((double) ic_statistics.totalCapacity) / ((double) ic_statistics.capacityCountingTime),
					(double) ((double) ic_statistics.totalBuffers) / ((double) ic_statistics.bufferCountingTime),
					ic_statistics.mismatchNum, ic_statistics.disorderedPktNum, ic_statistics.duplicatedPktNum,
					snd_control_info.cwnd, ic_statistics.statusQueryMsgNum);
	}

	ic_control_info.isSender = false;
	memset(&ic_statistics, 0, sizeof(ICStatistics));

	pthread_mutex_unlock(&ic_control_info.lock);

	/* reset the rx thread network error flag */
	resetRxThreadError();

	/* free sliceTable */
	if (this->sliceTable)
	{
		ICSliceTable *ic_tbl = this->sliceTable;
		for (int i = 0; i < ic_tbl->numSlices; ++i)
		{
			ICExecSlice *ic_slice = ic_tbl->slices + i;
			ic_free(ic_slice->children);
			ic_free(ic_slice->primaryProcesses);
		}
		ic_free(ic_tbl->slices);
		ic_free(ic_tbl);
	}

	this->activated = false;
	this->sliceTable = NULL;

	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_TERSE_IC)
		LOG(DEBUG1, "TeardownUDPIFCInterconnect_Internal successful");
}

void
TransportEntry::Broadcast(DataBlock *data, int *inactiveCountPtr)
{
	int			*p_inactive = inactiveCountPtr;
	int			index, inactive = 0;

	/* add our tcItem to each of the outgoing buffers. */
	index = Max(0, global_param.segindex); /* entry-db has -1 */
	for (int i = 0; i < this->numConns; i++, index++)
	{
		if (index >= this->numConns)
			index = 0;

		UDPConn *conn = this->GetConn(index);

		/* only send to still interested receivers. */
		if (conn->stillActive)
		{
			conn->Send(data);
			if (!conn->stillActive)
				inactive++;
		}
	}

	if (p_inactive != NULL)
		*p_inactive = (inactive ? 1 : 0);
}

TransportEntry*
CChunkTransportStateImpl::GetEntry(int motNodeID, bool checkValid)
{
	if (motNodeID > 0 && motNodeID <= static_cast<int>(this->entries_.size()))
	{
		TransportEntry *pEntry = this->entries_[motNodeID - 1].get();
		if (pEntry != nullptr)
		{
			if (!checkValid)
				return pEntry;
			if (pEntry->motNodeId == motNodeID && pEntry->valid)
				return pEntry;
		}
	}

	std::stringstream ss;
	ss << "ERROR, Interconnect Error: Unexpected Motion Node Id: " << motNodeID
	   << ". This means a motion node that wasn't setup is requesting interconnect resources.";
	throw ICInvalidIndex(ss.str(), __FILE__, __LINE__);
}

/*
 * The number of the Receiving Motion may be > 1, such as
 * Hashjoin
 *   -> Redis Motion
 *     ...
 *   -> Hash
 *     -> Redis Motion
 *       ...
 */
void
CChunkTransportStateImpl::CreateRecvEntries(ICSliceTable *sliceTable)
{
	ICExecSlice *mySlice = &sliceTable->slices[sliceTable->localSlice];

	/* now we'll do some setup for each of our Receiving Motion Nodes. */
	for (int child_index = 0; child_index < mySlice->numChildren; ++child_index)
	{
		int childId = mySlice->children[child_index];
		ICExecSlice *sendSlice = &sliceTable->slices[childId];

		if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
			LOG(DEBUG1, "Setup recving connections: my slice %d, childId %d", mySlice->sliceIndex, childId);

		if (sendSlice->sliceIndex > static_cast<int>(this->entries_.size()))
			this->entries_.resize(sendSlice->sliceIndex);
		this->checkMotNodeID(sendSlice->sliceIndex);

		std::unique_ptr<TransportEntry> pEntry =
			TransportEntry::MakeRecvEntry(this, sliceTable->ic_instance_id, sendSlice, mySlice);
		this->entries_[sendSlice->sliceIndex - 1] = std::move(pEntry);
	}
}

void
CChunkTransportStateImpl::CreateSendEntries(ICSliceTable *sliceTable)
{
	ICExecSlice *sendSlice = &sliceTable->slices[sliceTable->localSlice];

	if (sendSlice->parentIndex == -1) {
		ic_control_info.isSender = false;
		ic_control_info.lastExpirationCheckTime = 0;
		return;
	}

	snd_control_info.cwnd = 0;
	snd_control_info.minCwnd = 0;
	snd_control_info.ssthresh = 0;

	snd_buffer_pool.init();
	initUnackQueueRing(&unack_queue_ring);
	if (session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_LOSS_TIMER_IC)
		initUdpManager(&mudp);
	ic_control_info.isSender = true;
	ic_control_info.lastExpirationCheckTime = getCurrentTime();
	ic_control_info.lastPacketSendTime = ic_control_info.lastExpirationCheckTime;
	ic_control_info.lastDeadlockCheckTime = ic_control_info.lastExpirationCheckTime;

	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
		LOG(DEBUG1, "Interconnect seg%d slice%d setting up sending motion node",
					global_param.segindex, sendSlice->sliceIndex);

	if (sendSlice->sliceIndex > static_cast<int>(this->entries_.size()))
		this->entries_.resize(sendSlice->sliceIndex);
	this->checkMotNodeID(sendSlice->sliceIndex);

	ICExecSlice *recvSlice = &sliceTable->slices[sendSlice->parentIndex];
	std::unique_ptr<TransportEntry> pEntry =
		TransportEntry::MakeSendEntry(this, sliceTable->ic_instance_id, sendSlice, recvSlice);

	pEntry->txfd   = ICSenderSocket;
	pEntry->txport = ICSenderPort;
	pEntry->txfd_family = ICSenderFamily;

	snd_control_info.minCwnd = snd_control_info.cwnd;
	snd_control_info.ssthresh = snd_buffer_pool.maxCount;

#ifdef TRANSFER_PROTOCOL_STATS
	trans_proto_stats.init();
#endif

	this->entries_[sendSlice->sliceIndex - 1] = std::move(pEntry);
}

void
CChunkTransportStateImpl::DestroyRecvEntries(bool *isReceiver)
{
	ICExecSlice *mySlice = &this->sliceTable->slices[this->sliceId];
	for (int child_index = 0; child_index < mySlice->numChildren; ++child_index)
	{
		int childId = mySlice->children[child_index];
		ICExecSlice *aSlice = &this->sliceTable->slices[childId];

		/*
		 * First check whether the entry is initialized to avoid the potential
		 * errors thrown out from the removeChunkTransportState, which may
		 * introduce some memory leaks.
		 */
		int motNodeID = aSlice->sliceIndex;
		if (this->entries_[motNodeID - 1] == nullptr)
			continue;

		TransportEntry *pEntry = this->entries_[motNodeID - 1].get();
		Assert(motNodeID == pEntry->motNodeId);

		/* now it is safe to remove. */
		if (!pEntry->valid)
			continue;

#ifdef USE_ASSERT_CHECKING
		pEntry->dumpConnections("/tmp/receiving_entries");
#endif
		/* remove it */
		pEntry->valid = false;

		if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
			LOG(DEBUG1, "Interconnect closing connections from slice%d", aSlice->sliceIndex);

		*isReceiver = true;

		/*
		 * receivers know that they no longer care about data from
		 * below ... so we can safely discard data queued in both
		 * directions
		 */
		for (size_t i = 0; i < pEntry->conns_.size(); ++i)
		{
			UDPConn *conn = pEntry->conns_[i].get();

			Assert(conn);
			if (!conn)
				continue;

			if (conn->cdbProc == NULL)
				continue;

			/* out of memory has occurred, break out */
			if (!conn->pkt_q)
				break;

			rx_buffer_pool.maxCount -= conn->pkt_q_capacity;

			ic_control_info.connHtab.remove(conn);

			/*
			 * ReleaseBuffer() dequeues messages and moves
			 * them to pBuff
			 */
			while (conn->pkt_q_size > 0)
				conn->ReleaseBuffer(NULL);

			/* we also need to clear all the out-of-order packets */
			conn->freeDisorderedPackets();

			/* free up the packet queue */
			ic_free(conn->pkt_q);
			conn->pkt_q = NULL;

			if (global_param.destroyOpaqueDataCallback)
				global_param.destroyOpaqueDataCallback(&conn->opaque_data);

			if (conn->curBuff)
			{
				ic_free(conn->curBuff);
				conn->curBuff = NULL;
			}
		} // for conn

		Assert(!pEntry->conns);
	} // for entry
}

/*
 * computeNetworkStatistics
 * 		Compute the max/min/avg network statistics.
 */
static inline void
computeNetworkStatistics(uint64 value, uint64 *min, uint64 *max, double *sum)
{
	if (value >= *max)
		*max = value;
	if (value <= *min)
		*min = value;
	*sum += value;
}

void
CChunkTransportStateImpl::DestroySendEntries()
{
	ICExecSlice *mySlice = &this->sliceTable->slices[this->sliceId];
	if (mySlice->parentIndex == -1)
		return;

	/* cleanup a Sending motion node. */
	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
	{
		ICExecSlice *parentSlice = &this->sliceTable->slices[mySlice->parentIndex];
		LOG(DEBUG1, "Interconnect seg%d slice%d closing connections to slice%d (%d peers)",
					global_param.segindex, mySlice->sliceIndex, mySlice->parentIndex,
					parentSlice->numPrimaryProcesses);
	}

	/*
	 * In the olden days, we required that the error case successfully
	 * transmit and end-of-stream message here. But the introduction of
	 * cdbdisp_check_estate_for_cancel() alleviates for the QD case, and
	 * the cross-connection of writer gangs in the dispatcher (propagation
	 * of cancel between them) fixes the I-S case.
	 *
	 * So the call to forceEosToPeers() is no longer required.
	 */
	int motNodeID = mySlice->sliceIndex;
	if (this->entries_[motNodeID - 1] == nullptr)
		return;

	TransportEntry *pEntry = this->entries_[motNodeID - 1].get();
	Assert(motNodeID == pEntry->motNodeId);

	/* now it is safe to remove. */
	if (!pEntry->valid)
		return;

#ifdef USE_ASSERT_CHECKING
	pEntry->dumpConnections("/tmp/sending_entries");
#endif

	/* remove it */
	pEntry->valid = false;

	uint64		maxRtt = 0;
	double		avgRtt = 0;
	uint64		minRtt = ~((uint64) 0);

	uint64		maxDev = 0;
	double		avgDev = 0;
	uint64		minDev = ~((uint64) 0);

	/* connection array allocation may fail in interconnect setup. */
	for (size_t i = 0; i < pEntry->conns_.size(); ++i)
	{
		UDPConn *conn = pEntry->conns_[i].get();

		Assert(conn);
		if (!conn)
			continue;

		if (!conn->cdbProc)
			continue;

		/* compute some statistics */
		computeNetworkStatistics(conn->rtt, &minRtt, &maxRtt, &avgRtt);
		computeNetworkStatistics(conn->dev, &minDev, &maxDev, &avgDev);

		conn->sndQueue.release(false);
		conn->unackQueue.release(session_param.Gp_interconnect_fc_method == INTERCONNECT_FC_METHOD_CAPACITY_IC ? false : true);

		ic_control_info.connHtab.remove(conn);

		if (conn->curBuff)
		{
			ic_free(conn->curBuff);
			conn->curBuff = NULL;
		}
	}
	avgRtt = avgRtt / pEntry->numConns;
	avgDev = avgDev / pEntry->numConns;

	/* free all send side buffers */
	snd_buffer_pool.clean();

	Assert(!pEntry->conns);

#ifdef TRANSFER_PROTOCOL_STATS
	trans_proto_stats.dump();
#endif

	if (IC_DEBUG1 >= session_param.log_min_messages)
	{
		LOG(DEBUG1, "Interconnect State: isSender %d DEFAULT_RTT %d rtt/dev [%lu/%lu, %f/%f, %lu/%lu] ",
					ic_control_info.isSender, DEFAULT_RTT, (minRtt == ~((uint64) 0) ? 0 : minRtt),
					(minDev == ~((uint64) 0) ? 0 : minDev), avgRtt, avgDev, maxRtt, maxDev);
	}
}

void
CChunkTransportStateImpl::checkMotNodeID(int sendMotNodeID)
{
	Assert(sendMotNodeID > 0);

	if ((sendMotNodeID) <= 0) {
		std::stringstream ss;
		ss <<"ERROR, Interconnect Error: Unexpected Motion Node Id: "<<sendMotNodeID<<
			". This means a motion node that wasn't setup is requesting interconnect resources.";
		throw ICInvalidIndex(ss.str(), __FILE__, __LINE__);
	}

	TransportEntry *pEntry = this->entries_[sendMotNodeID - 1].get();
	if (pEntry != nullptr) {
		UDPConn *conn = pEntry->conns_.size() ? pEntry->conns_[0].get() : nullptr;

		Assert(static_cast<size_t>(pEntry->numConns) == pEntry->conns_.size());
		std::stringstream ss;
		ss <<"ERROR, interconnect error: A HTAB entry for motion node "<<sendMotNodeID<<
			" already exists numConns "<<pEntry->conns_.size()<<
			" first sock " << (conn != NULL ? conn->sockfd : -2);
		throw ICInvalidIndex(ss.str(), __FILE__, __LINE__);
	}
}

CChunkTransportStateImpl::CChunkTransportStateImpl(ICSliceTable *_sliceTable)
{
	activated       = false;
	teardownActive  = false;

	sliceTable      = _sliceTable;
	sliceId         = sliceTable->localSlice;
	icInstanceId    = sliceTable->ic_instance_id;

	networkTimeoutIsLogged = false;

	clientState     = NULL;
}

ICChunkTransportState*
CChunkTransportStateImpl::SetupUDP(ICSliceTable *sliceTable, SessionMotionLayerIPCParam *param)
{
	if (param)
		memcpy(&session_param, param, sizeof(*param));

	/*
	 * The rx-thread might have set an error since last teardown,
	 * technically it is not part of current query, discard it directly.
	 */
	resetRxThreadError();

	try {
		ICChunkTransportState *state = CChunkTransportStateImpl::setup(sliceTable);

		/* Internal error if we locked the mutex but forgot to unlock it. */
		Assert(pthread_mutex_unlock(&ic_control_info.lock) != 0);

		return state;

	} catch (...) {
		/*
		 * Remove connections from hash table to avoid packet handling in the
		 * rx pthread, else the packet handling code could use memory whose
		 * context (InterconnectContext) would be soon reset - that could
		 * panic the process.
		 */
		ConnHashTable *ht = &ic_control_info.connHtab;

		for (int i = 0; i < ht->size; i++)
		{
			struct ConnHtabBin *trash;
			UDPConn *conn = NULL;

			trash = ht->table[i];
			while (trash != NULL)
			{
				conn = trash->conn;
				/* Get trash at first as trash will be pfree-ed in remove. */
				trash = trash->next;
				ht->remove(conn);
			}
		}
		pthread_mutex_unlock(&ic_control_info.lock);

		throw;
	}
}

void
CChunkTransportStateImpl::TeardownUDP(bool hasErrors)
{
	try {
		CChunkTransportStateImpl::state_ = nullptr;
		this->teardown(hasErrors);
		delete this;
		Assert(pthread_mutex_unlock(&ic_control_info.lock) != 0);
	} catch (...) {
		pthread_mutex_unlock(&ic_control_info.lock);
		throw;
	}
}

CChunkTransportState **
CChunkTransportStateImpl::GetTransportState()
{
	return &CChunkTransportStateImpl::state_;
}

void
CChunkTransportStateImpl::RecvRoute(int16 motNodeID,
												   int16 srcRoute,
												   GetDataLenInPacket getLen,
												   DataBlock *data)
{
	try {
		TransportEntry *pEntry = GetEntry(motNodeID, true);
		pEntry->RecvRoute(srcRoute, getLen, data);

		/* error if mutex still held (debug build only) */
		Assert(pthread_mutex_unlock(&ic_control_info.lock) != 0);
	} catch (...) {
		pthread_mutex_unlock(&ic_control_info.lock);
		throw;
	}
}

void
CChunkTransportStateImpl::RecvAny(int16 motNodeID,
								int16 *srcRoute,
								GetDataLenInPacket getLen,
								DataBlock *data)
{
	try {
		TransportEntry *pEntry = GetEntry(motNodeID, true);
		pEntry->RecvAny(srcRoute, getLen, data);

		/* error if mutex still held (debug build only) */
		Assert(pthread_mutex_unlock(&ic_control_info.lock) != 0);
	} catch (...) {
		pthread_mutex_unlock(&ic_control_info.lock);
		throw;
	}
}

void
CChunkTransportStateImpl::SendEOS(int motNodeID, DataBlock *data)
{
	int			retry = 0;
	int			activeCount = 0;
	int			timeout = 0;

	TransportEntry *pEntry = GetEntry(motNodeID, true);

	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
		LOG(DEBUG1, "Interconnect seg%d slice%d sending end-of-stream to slice%d",
					global_param.segindex, motNodeID, pEntry->recvSlice->sliceIndex);

	/*
	 * we want to add our tcItem onto each of the outgoing buffers -- this is
	 * guaranteed to leave things in a state where a flush is *required*.
	 */
	pEntry->Broadcast(data, NULL);

	pEntry->sendingEos = true;

	uint64		now = getCurrentTime();

	/* now flush all of the buffers. */
	for (int i = 0; i < pEntry->numConns; i++)
	{
		UDPConn *conn = pEntry->GetConn(i);
		if (conn->stillActive)
		{
			if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
				LOG(DEBUG1, "sent eos to route %d tuplecount %d seq %d flags 0x%x stillActive %s icId %d %d",
							conn->route, conn->tupleCount, conn->conn_info.seq,
							conn->conn_info.flags, (conn->stillActive ? "true" : "false"),
							conn->conn_info.icId, conn->msgSize);

			/* prepare this for transmit */
			conn->conn_info.flags |= UDPIC_FLAGS_EOS;

			/* place it into the send queue */
			conn->prepareXmit();
			conn->sndQueue.append(conn->curBuff);
			conn->sendBuffers();
			conn->curBuff = NULL;
			conn->pBuff = NULL;

			conn->tupleCount = 0;
			conn->msgSize = sizeof(conn->conn_info);
			conn->deadlockCheckBeginTime = now;

			activeCount++;
		}
	}

	/*
	 * Now waiting for acks from receivers.
	 *
	 * Note here waiting is done in a separate phase from the EOS sending
	 * phase to make the processing faster when a lot of connections are slow
	 * and have frequent packet losses. In fault injection tests, we found
	 * this.
	 *
	 */
	while (activeCount > 0)
	{
		activeCount = 0;

		for (int i = 0; i < pEntry->numConns; i++)
		{
			UDPConn *conn = pEntry->GetConn(i);
			if (conn->stillActive)
			{
				retry = 0;
				ic_control_info.lastPacketSendTime = 0;

				/* wait until this queue is emptied */
				while (conn->unackQueue.length() > 0 ||
					   conn->sndQueue.length() > 0)
				{
					timeout = conn->computeTimeout(retry);

					if (pEntry->pollAcks(timeout))
						pEntry->handleAcks(true);

					conn->checkExceptions(retry++, timeout);

					if (retry >= MAX_TRY)
					{
						if (conn->unackQueue.length() == 0)
							conn->sendBuffers();
						break;
					}
				}

				if ((!conn->cdbProc) || (conn->unackQueue.length() == 0 &&
										 conn->sndQueue.length() == 0))
				{
					conn->state = mcsEosSent;
					conn->stillActive = false;
				}
				else
					activeCount++;
			}
		}
	}

	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
		LOG(DEBUG1, "SendEOSleaving, activeCount %d", activeCount);
}

void
CChunkTransportStateImpl::SendStop(int16 motNodeID)
{
	Assert(this->activated);

	TransportEntry *pEntry = GetEntry(motNodeID, true);

	/*
	 * Note: we're only concerned with receivers here.
	 */
	pthread_mutex_lock(&ic_control_info.lock);

	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
		LOG(DEBUG1, "Interconnect needs no more input from slice%d; notifying senders to stop.", motNodeID);

	for (int i = 0; i < pEntry->numConns; i++)
	{
		UDPConn *conn = pEntry->GetConn(i);

		/*
		 * Note here, the stillActive flag of a connection may have been set
		 * to false by DeactiveConn.
		 */
		if (conn->stillActive)
		{
			if (conn->conn_info.flags & UDPIC_FLAGS_EOS)
			{
				/*
				 * we have a queued packet that has EOS in it. We've acked it,
				 * so we're done
				 */
				if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
					LOG(DEBUG1, "do sendstop: already have queued EOS packet, we're done. node %d route %d", motNodeID, i);

				conn->stillActive = false;

				/* need to drop the queues in the teardown function. */
				while (conn->pkt_q_size > 0)
					conn->ReleaseBuffer(NULL);
			}
			else
			{
				conn->stopRequested = true;
				conn->conn_info.flags |= UDPIC_FLAGS_STOP;

				/*
				 * The peer addresses for incoming connections will not be set
				 * until the first packet has arrived. However, when the lower
				 * slice does not have data to send, the corresponding peer
				 * address for the incoming connection will never be set. We
				 * will skip sending ACKs to those connections.
				 */

#ifdef FAULT_INJECTOR
				if (FaultInjector_InjectFaultIfSet(
												   "interconnect_stop_ack_is_lost",
												   DDLNotSpecified,
												   "" /* databaseName */ ,
												   "" /* tableName */ ) == FaultInjectorTypeSkip)
				{
					continue;
				}
#endif

				if (conn->peer.ss_family == AF_INET || conn->peer.ss_family == AF_INET6)
				{
					uint32		seq = conn->conn_info.seq > 0 ? conn->conn_info.seq - 1 : 0;

					conn->sendAck(UDPIC_FLAGS_STOP | UDPIC_FLAGS_ACK | UDPIC_FLAGS_CAPACITY | conn->conn_info.flags, seq, seq);

					if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
						LOG(DEBUG1, "sent stop message. node %d route %d seq %d", motNodeID, i, seq);
				}
				else
				{
					if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
						LOG(DEBUG1, "first packet did not arrive yet. don't sent stop message. node %d route %d", motNodeID, i);
				}
			}
		}
	}
	pthread_mutex_unlock(&ic_control_info.lock);
}

void
TransportEntry::Send(int16 targetRoute, DataBlock *db, bool broadcast, int *recount)
{
#ifdef AMS_VERBOSE_LOGGING
	LOG(DEBUG5, "SendTupleChunkToAMS: chunk length %d", db->len);
#endif

	if (broadcast)
	{
		this->Broadcast(db, recount);
	}
	else
	{
		if (targetRoute < 0 || targetRoute >= this->numConns)
		{
			std::stringstream ss;
			ss << "FATAL: SendTupleChunkToAMS: targetRoute is " << targetRoute
			   << ", must be between 0 and " << this->numConns << " .";
			throw ICFatalException(ss.str(), __FILE__, __LINE__);
		}

		/* handle pt-to-pt message. Primary */
		UDPConn *conn = this->GetConn(targetRoute);

		/* only send to interested connections */
		if (conn->stillActive)
		{
			conn->Send(db);
			if (!conn->stillActive)
				*recount = 1;
		}
		/* in 4.0 logical mirror xmit eliminated. */
	}
}

bool
TransportEntry::SendData(int16 targetRoute, DataBlock *pblocks, int num, bool broadcast)
{
	int recount = 0;

	int payload = global_param.Gp_max_packet_size - sizeof(icpkthdr);

	/*
	 * tcItem can actually be a chain of tcItems.  we need to send out all of
	 * them.
	 */
	for (int i = 0; i < num; ++i)
	{
		DataBlock db = *(pblocks + i);
		while (db.len > 0)
		{
			DataBlock toSend;
			toSend.len = db.len > payload ? payload : db.len;
			toSend.pos = db.pos;
			this->Send(targetRoute, &toSend, broadcast, &recount);
			db.len -= toSend.len;
			db.pos += toSend.len;
		}
	}

	if (recount == 0)
		return true;

	/* if we don't have any connections active, return false */
	int i = 0;
	for (i = 0; i < this->numConns; i++)
	{
		UDPConn *conn = this->GetConn(i);
		if (conn->stillActive)
			break;
	}

	/* if we found an active connection we're not done */
	return (i < this->numConns);

}

bool
CChunkTransportStateImpl::SendData(int16 motNodeID,
								int16 targetRoute,
								DataBlock *pblocks,
								int num,
								bool broadcast)
{
	TransportEntry *pEntry = GetEntry(motNodeID, true);
	return pEntry->SendData(targetRoute, pblocks, num, broadcast);
}

void
CChunkTransportStateImpl::GetFreeSpace(int16 motNodeID, int16 targetRoute, BufferBlock *b)
{
	Assert(b != NULL);

	TransportEntry *pEntry = GetEntry(motNodeID, true);

		/* handle pt-to-pt message. Primary */
	UDPConn *conn = pEntry->GetConn(targetRoute);

	b->pos = NULL;
	b->len = 0;
	if (conn->stillActive)
	{
		b->pos = conn->pBuff + conn->msgSize;
		Assert(global_param.Gp_max_packet_size >= conn->msgSize);
		b->len = global_param.Gp_max_packet_size - conn->msgSize;
	}
}

void
CChunkTransportStateImpl::ReduceFreeSpace(int16 motNodeID, int16 targetRoute, int length)
{
	TransportEntry *pEntry = GetEntry(motNodeID, true);

	/* handle pt-to-pt message. Primary */
	UDPConn *conn = pEntry->GetConn(targetRoute);

	/* only send to interested connections */
	if (conn->stillActive)
	{
		Assert(conn->msgSize + length <= global_param.Gp_max_packet_size);
		conn->msgSize += length;
		conn->tupleCount++;
	}
}

void
CChunkTransportStateImpl::ReleaseAndAck(int motNodeID, int route)
{
	AckSendParam param;

	TransportEntry *pEntry = GetEntry(motNodeID, true);
	UDPConn *conn = pEntry->GetConn(route);

	memset(&param, 0, sizeof(AckSendParam));

	pthread_mutex_lock(&ic_control_info.lock);

	if (conn->pBuff != NULL)
	{
		conn->ReleaseBuffer(&param);
	}
	else
	{
		pthread_mutex_unlock(&ic_control_info.lock);
		throw ICFatalException("FATAL: Interconnect error: tried to release a NULL buffer", __FILE__, __LINE__);
	}

	pthread_mutex_unlock(&ic_control_info.lock);

	/*
	 * real ack sending is after lock release to decrease the lock holding
	 * time.
	 */
	if (param.msg.len != 0)
		UDPConn::sendAckWithParam(&param);
}

void
CChunkTransportStateImpl::DeactiveRoute(int motNodeID, int srcRoute, const char *reason)
{
	TransportEntry *pEntry = GetEntry(motNodeID, true);
	UDPConn *conn = pEntry->GetConn(srcRoute);

	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
	{
		LOG(DEBUG3, "Interconnect finished receiving from seg%d slice%d %s pid=%d sockfd=%d; %s",
					conn->remoteContentId, pEntry->sendSlice->sliceIndex, conn->remoteHostAndPort,
					conn->cdbProc->pid, conn->sockfd, reason);
	}

#ifdef AMS_VERBOSE_LOGGING
	LOG(INFO, "deregisterReadInterest set stillactive = false for node %d route %d (%s)", motNodeID, srcRoute, reason);
#endif

	conn->DeactiveConn();
}

void*
CChunkTransportStateImpl::GetOpaqueDataInConn(int16 motNodeID, int16 targetRoute)
{
	TransportEntry *pEntry = this->GetEntry(motNodeID, true);
	UDPConn *conn = pEntry->GetConn(targetRoute);
	return conn->opaque_data;
}

int32*
CChunkTransportStateImpl::GetSentRecordTypmodInConn(int16 motNodeID, int16 targetRoute)
{
	TransportEntry *pEntry = this->GetEntry(motNodeID, true);
	UDPConn *conn = pEntry->GetConn(targetRoute);
	return &conn->sent_record_typmod;
}

int
CChunkTransportStateImpl::GetConnNum(int motNodeID) {
	TransportEntry *pEntry = this->GetEntry(motNodeID, true);
	return pEntry->conns_.size();
}

void
CChunkTransportStateImpl::NotifyQuit() {
	thread_quit = true;
}

void
CChunkTransportStateImpl::SetVectorEngineAsUser() {
	vector_engine_is_user = true;
}

CChunkTransportStateImpl* ToDerived(CChunkTransportState *ptr) {
	return static_cast<CChunkTransportStateImpl*>(ptr);
}

/*
 * C++ interface wrapper class based on class CChunkTransportStateImpl;
 */
ICChunkTransportState*
CChunkTransportState::SetupUDP(ICSliceTable *sliceTable,
											  SessionMotionLayerIPCParam *param) {
	return CChunkTransportStateImpl::SetupUDP(sliceTable, param);
}

void
CChunkTransportState::TeardownUDP(bool hasErrors) {
	ToDerived(this)->TeardownUDP(hasErrors);
}

void
CChunkTransportState::RecvRoute(int16 motNodeID, int16 srcRoute,
											GetDataLenInPacket getLen, DataBlock *data) {
	ToDerived(this)->RecvRoute(motNodeID, srcRoute, getLen, data);
}

void
CChunkTransportState::RecvAny(int16 motNodeID, int16 *srcRoute,
										GetDataLenInPacket getLen, DataBlock *data) {
	ToDerived(this)->RecvAny(motNodeID, srcRoute, getLen, data);
}

void
CChunkTransportState::SendEOS(int motNodeID, DataBlock *data) {
	ToDerived(this)->SendEOS(motNodeID, data);
}

void
CChunkTransportState::SendStop(int16 motNodeID) {
	return ToDerived(this)->SendStop(motNodeID);
}

bool
CChunkTransportState::SendData(int16 motNodeID, int16 targetRoute, DataBlock *pblocks,
											 int num, bool broadcast) {
	return ToDerived(this)->SendData(motNodeID, targetRoute, pblocks, num, broadcast);
}

void
CChunkTransportState::GetFreeSpace(int16 motNodeID, int16 targetRoute, BufferBlock *b) {
	return ToDerived(this)->GetFreeSpace(motNodeID, targetRoute, b);
}

void
CChunkTransportState::ReduceFreeSpace(int16 motNodeID, int16 targetRoute, int length) {
	return ToDerived(this)->ReduceFreeSpace(motNodeID, targetRoute, length);
}

void
CChunkTransportState::ReleaseAndAck(int motNodeID, int route) {
	return ToDerived(this)->ReleaseAndAck(motNodeID, route);
}

void
CChunkTransportState::DeactiveRoute(int motNodeID, int srcRoute, const char *reason) {
	return ToDerived(this)->DeactiveRoute(motNodeID, srcRoute, reason);
}

void*
CChunkTransportState::GetOpaqueDataInConn(int16 motNodeID, int16 targetRoute) {
	return ToDerived(this)->GetOpaqueDataInConn(motNodeID, targetRoute);
}

int32*
CChunkTransportState::GetSentRecordTypmodInConn(int16 motNodeID, int16 targetRoute) {
	return ToDerived(this)->GetSentRecordTypmodInConn(motNodeID, targetRoute);
}

int
CChunkTransportState::GetConnNum(int motNodeID) {
	return ToDerived(this)->GetConnNum(motNodeID);
}

void
CChunkTransportState::NotifyQuit() {
	return ToDerived(this)->NotifyQuit();
}

void
CChunkTransportState::SetVectorEngineAsUser() {
	return ToDerived(this)->SetVectorEngineAsUser();
}

CChunkTransportState**
CChunkTransportState::GetTransportState() {
	return CChunkTransportStateImpl::GetTransportState();
}

/*
 * C interface wrapper of UDP implement based C++ interface calss CChunkTransportState.
 */
#ifdef __cplusplus
extern "C" {
#endif

static void handleException()
{
	try{
		throw;
	} catch (const std::bad_alloc &e) {
		SetLastError(LEVEL_ERROR, "out of memory");
	} catch (const ICFatalException &e) {
		SetLastError(LEVEL_FATAL, e.msg());
	} catch (const ICException &e) {
		SetLastError(LEVEL_ERROR, e.msg());
	} catch (const std::exception &e) {
		SetLastError(LEVEL_ERROR, e.what());
	} catch (...) {
		SetLastError(LEVEL_ERROR, "something unknown wrong happened!");
	}
}

ICChunkTransportState*
UDP2_SetupUDP(ICSliceTable *sliceTable, SessionMotionLayerIPCParam *param)
{
	try {
		return CChunkTransportState::SetupUDP(sliceTable, param);
	} catch (...) {
		handleException();
	}

	return NULL;
}

/*
 * TeardownUDPIFCInterconnect
 * 		Tear down UDP interconnect.
 *
 * This function is called to release the resources used by interconnect.
 */
void
UDP2_TeardownUDP(ICChunkTransportState *transportStates,
			bool hasErrors)
{
	Assert(transportStates);

	try {
		CChunkTransportState *cstate = static_cast<CChunkTransportState*>(transportStates);
		cstate->TeardownUDP(hasErrors);
	} catch (...) {
		handleException();
	}
}

/*
 * RecvTupleChunkFromUDPIFC
 * 		Receive tuple chunks from a specific route (connection)
 */
void
UDP2_RecvRoute(ICChunkTransportState *transportStates,
		  int16 motNodeID,
		  int16 srcRoute,
		  GetDataLenInPacket getLen,
		  DataBlock *data)
{
	Assert(transportStates);

	try {
		CChunkTransportState *cstate = static_cast<CChunkTransportState*>(transportStates);
		cstate->RecvRoute(motNodeID, srcRoute, getLen, data);
	} catch (...) {
		handleException();
	}
}

/*
 * RecvTupleChunkFromAnyUDPIFC
 * 		Receive tuple chunks from any route (connections)
 */
void
UDP2_RecvAny(ICChunkTransportState *transportStates,
		int16 motNodeID,
		int16 *srcRoute,
		GetDataLenInPacket getLen,
		DataBlock *data)

{
	Assert(transportStates);

	try {
		CChunkTransportState *cstate = static_cast<CChunkTransportState*>(transportStates);
		cstate->RecvAny(motNodeID, srcRoute, getLen, data);
	} catch (...) {
		handleException();
	}
}

/*
 * SendEOS
 * 		broadcast eos messages to receivers.
 */
void
UDP2_SendEOS(ICChunkTransportState *transportStates,
			  int motNodeID,
			  DataBlock *data)
{
	Assert(transportStates);

	try {
		CChunkTransportState *cstate = static_cast<CChunkTransportState*>(transportStates);
		cstate->SendEOS(motNodeID, data);
	} catch (...) {
		handleException();
	}
}

void
UDP2_SendStop(ICChunkTransportState *transportStates,
					  int16 motNodeID)
{
	Assert(transportStates);

	try {
		CChunkTransportState *cstate = static_cast<CChunkTransportState*>(transportStates);
		cstate->SendStop(motNodeID);
	} catch (...) {
		handleException();
	}
}

bool
UDP2_SendData(ICChunkTransportState *transportStates,
		int16 motNodeID,
		int16 targetRoute,
		DataBlock *pblocks,
		int num,
		bool broadcast)
{
	Assert(transportStates);

	try {
		CChunkTransportState *cstate = static_cast<CChunkTransportState*>(transportStates);
		return cstate->SendData(motNodeID, targetRoute, pblocks, num, broadcast);
	} catch (...) {
		handleException();
	}

	return false;
}

/*
 * The fetches a direct pointer into our transmit buffers, along with
 * an indication as to how much data can be safely shoved into the
 * buffer (started at the pointed location).
 *
 * This works a lot like SendTupleChunkToAMS().
 */
void
UDP2_GetFreeSpace(ICChunkTransportState *transportStates,
							int16 motNodeID,
							int16 targetRoute,
							BufferBlock *b)
{
	Assert(transportStates);

	try {
		CChunkTransportState *cstate = static_cast<CChunkTransportState*>(transportStates);
		cstate->GetFreeSpace(motNodeID, targetRoute, b);
	} catch (...) {
		handleException();
	}
}

void
UDP2_ReduceFreeSpace(ICChunkTransportState *transportStates,
							int16 motNodeID,
							int16 targetRoute,
							int length)
{
	Assert(transportStates);

	try {
		CChunkTransportState *cstate = static_cast<CChunkTransportState*>(transportStates);
		cstate->ReduceFreeSpace(motNodeID, targetRoute, length);
	} catch (...) {
		handleException();
	}
}

/*
 * SendAck
 *
 * The cdbmotion code has discarded our pointer to the motion-conn
 * structure, but has enough info to fully specify it.
 */
void
UDP2_ReleaseAndAck(ICChunkTransportState *transportStates,
			  int motNodeID,
			  int route)
{
	Assert(transportStates);

	try {
		CChunkTransportState *cstate = static_cast<CChunkTransportState*>(transportStates);
		cstate->ReleaseAndAck(motNodeID, route);
	} catch (...) {
		handleException();
	}
}

void
UDP2_DeactiveRoute(ICChunkTransportState *transportStates, int motNodeID, int
		srcRoute, const char *reason) { Assert(transportStates);

	try { CChunkTransportState *cstate =
		static_cast<CChunkTransportState*>(transportStates);
		cstate->DeactiveRoute(motNodeID, srcRoute, reason);
	} catch (...) {
		handleException();
	}
}

void*
UDP2_GetOpaqueDataInConn(ICChunkTransportState *transportStates,
							 int16 motNodeID,
							 int16 targetRoute)
{
	Assert(transportStates);

	try {
		CChunkTransportState *cstate = static_cast<CChunkTransportState*>(transportStates);
		return cstate->GetOpaqueDataInConn(motNodeID, targetRoute);
	} catch (...) {
		handleException();
	}

	return NULL;
}

int32*
UDP2_GetSentRecordTypmodInConn(ICChunkTransportState *transportStates,
								int16 motNodeID,
								int16 targetRoute)
{
	Assert(transportStates);

	try {
		CChunkTransportState *cstate = static_cast<CChunkTransportState*>(transportStates);
		return cstate->GetSentRecordTypmodInConn(motNodeID, targetRoute);
	} catch (...) {
		handleException();
	}

	return NULL;
}

void
UDP2_InitUDPIFC(GlobalMotionLayerIPCParam *param)
{
	if (param)
	{
		if (global_param.interconnect_address)
			free(global_param.interconnect_address);

		memcpy(&global_param, param, sizeof(*param));
		global_param.interconnect_address = strdup(param->interconnect_address);
	}

	try {
		InitMotionUDPIFC(&UDP_listenerFd, &udp_listener_port);

		if (IC_DEBUG1 >= session_param.log_min_messages)
			LOG(DEBUG1, "Interconnect listening on udp port %d ", udp_listener_port);

	} catch (...) {
		handleException();
	}
}

void
UDP2_CleanUpUDPIFC(void)
{
	if (session_param.gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG_IC)
		LOG(DEBUG3, "Cleaning Up Motion Layer IPC...");

	try {
		CleanupMotionUDPIFC();
	} catch (...) {
		handleException();
	}

	if (UDP_listenerFd >= 0)
		closesocket(UDP_listenerFd);

	/* be safe and reset global state variables. */
	udp_listener_port = 0;
	UDP_listenerFd    = -1;
}

void
UDP2_WaitQuitUDPIFC(void)
{
	/*
	 * Just in case ic thread is waiting on the locks.
	 */
	pthread_mutex_unlock(&ic_control_info.lock);

	ic_atomic_write_u32(&ic_control_info.shutdown, 1);

	if (ic_control_info.threadCreated)
	{
		SendDummyPacket();
		pthread_join(ic_control_info.threadHandle, NULL);
	}
	ic_control_info.threadCreated = false;
}

uint32
UDP2_GetActiveConns(void)
{
	return ic_statistics.activeConnectionsNum;
}

int
UDP2_GetICHeaderSizeUDP(void)
{
	return sizeof(struct icpkthdr);
}

int32
UDP2_GetListenPortUDP(void)
{
	return udp_listener_port;
}

#ifdef __cplusplus
} // extern "C"
#endif