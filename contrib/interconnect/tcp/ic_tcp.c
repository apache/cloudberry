/*-------------------------------------------------------------------------
 * ic_tcp.c
 *	   Interconnect code specific to TCP transport.
 *
 * Portions Copyright (c) 2005-2008, Greenplum, Inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 2023, HashData Technology Limited.
 *
 *
 * IDENTIFICATION
 *		contrib/interconnect/tcp/ic_tcp.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "ic_tcp.h"
#include "ic_internal.h"
#include "ic_common.h"
#include "common/ip.h"
#include "nodes/execnodes.h"	/* ExecSlice, SliceTable */
#include "nodes/pg_list.h"
#include "nodes/print.h"
#include "miscadmin.h"
#include "libpq/libpq-be.h"
#include "postmaster/postmaster.h"
#include "utils/builtins.h"

#include "cdb/cdbselect.h"
#include "cdb/tupchunklist.h"
#include "cdb/ml_ipc.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp.h"

#ifdef ENABLE_IC_PROXY
#include "proxy/ic_proxy_backend.h"
#endif							/* ENABLE_IC_PROXY */

#include <fcntl.h>
#include <limits.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <netinet/in.h>

#define USECS_PER_SECOND 1000000
#define MSECS_PER_SECOND 1000

/*
 * GpMonotonicTime: used to guarantee that the elapsed time is in
 * the monotonic order between two gp_get_monotonic_time calls.
 */
typedef struct GpMonotonicTime
{
	struct timeval beginTime;
	struct timeval endTime;
}			GpMonotonicTime;

static void gp_set_monotonic_begin_time(GpMonotonicTime * time);
static void gp_get_monotonic_time(GpMonotonicTime * time);
static inline uint64 gp_get_elapsed_ms(GpMonotonicTime * time);
static inline uint64 gp_get_elapsed_us(GpMonotonicTime * time);
static inline int timeCmp(struct timeval *t1, struct timeval *t2);

/*
 * backlog for listen() call: it is important that this be something like a
 * good match for the maximum number of QEs. Slow insert performance will
 * result if it is too low.
 */
#define CONNECT_RETRY_MS	4000
#define CONNECT_AGGRESSIVERETRY_MS	500

/* listener backlog is calculated at listener-creation time */
extern int	listenerBacklog;

/* our timeout value for select() and other socket operations. */
static struct timeval tval;

/* TCP listen fd */
int			TCP_listenerFd;

/* TCP listen port */
int32		tcp_listener_port;


static ChunkTransportStateEntry * startOutgoingConnections(ChunkTransportState * transportStates,
														   ExecSlice * sendSlice,
														   int *pOutgoingCount);

static void format_fd_set(StringInfo buf, int nfds, mpp_fd_set * fds, char *pfx, char *sfx);
static void setupOutgoingConnection(ChunkTransportState * transportStates,
									ChunkTransportStateEntry * pEntry, MotionConn * conn);
static void updateOutgoingConnection(ChunkTransportState * transportStates,
									 ChunkTransportStateEntry * pEntry, MotionConn * conn, int errnoSave);
static void sendRegisterMessage(ChunkTransportState * transportStates, ChunkTransportStateEntry * pEntry, MotionConn * conn);
static bool readRegisterMessage(ChunkTransportState * transportStates,
								MotionConn * conn);
static MotionConn * acceptIncomingConnection(void);

static void flushInterconnectListenerBacklog(void);

static void waitOnOutbound(ChunkTransportStateEntry * pEntry);

static bool flushBuffer(ChunkTransportState * transportStates,
						ChunkTransportStateEntry * pEntry, MotionConn * conn, int16 motionId);

#ifdef AMS_VERBOSE_LOGGING
static void dumpEntryConnections(int elevel, ChunkTransportStateEntry * pEntry);
static void print_connection(ChunkTransportState * transportStates, int fd, const char *msg);
#endif

/*
 * setupTCPListeningSocket
 */
static void
setupTCPListeningSocket(int backlog, int *listenerSocketFd, int32 *listenerPort)
{
	int			errnoSave;
	int			fd = -1;
	const char *fun;

	*listenerSocketFd = -1;
	*listenerPort = 0;

	struct sockaddr_storage addr;
	socklen_t	addrlen;

	struct addrinfo hints;
	struct addrinfo *addrs,
			   *rp;
	int			s;
	char		service[32];

	/*
	 * we let the system pick the TCP port here so we don't have to manage
	 * port resources ourselves.  So set the port to 0 (any port)
	 */
	snprintf(service, 32, "%d", 0);
	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC;	/* Allow IPv4 or IPv6 */
	hints.ai_socktype = SOCK_STREAM;	/* Two-way, out of band connection */
	hints.ai_protocol = 0;		/* Any protocol - TCP implied for network use
								 * due to SOCK_STREAM */

	if (Gp_interconnect_address_type == INTERCONNECT_ADDRESS_TYPE_UNICAST)
	{
		Assert(interconnect_address && strlen(interconnect_address) > 0);
		hints.ai_flags |= AI_NUMERICHOST;
		ereportif(gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG, DEBUG3,
				  (errmsg("getaddrinfo called with unicast address: %s",
						  interconnect_address)));
	}
	else
	{
		Assert(interconnect_address == NULL);
		hints.ai_flags |= AI_PASSIVE;
		ereportif(gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG, DEBUG3,
				  (errmsg("getaddrinfo called with wildcard address")));
	}

	s = getaddrinfo(interconnect_address, service, &hints, &addrs);
	if (s != 0)
		elog(ERROR, "getaddrinfo says %s", gai_strerror(s));

	/*
	 * getaddrinfo() returns a list of address structures, one for each valid
	 * address and family we can use.
	 *
	 * Try each address until we successfully bind. If socket (or bind) fails,
	 * we (close the socket and) try the next address.  This can happen if the
	 * system supports IPv6, but IPv6 is disabled from working, or if it
	 * supports IPv6 and IPv4 is disabled.
	 */


	/*
	 * If there is both an AF_INET6 and an AF_INET choice, we prefer the
	 * AF_INET6, because on UNIX it can receive either protocol, whereas
	 * AF_INET can only get IPv4.  Otherwise we'd need to bind two sockets,
	 * one for each protocol.
	 *
	 * Why not just use AF_INET6 in the hints?  That works perfect if we know
	 * this machine supports IPv6 and IPv6 is enabled, but we don't know that.
	 */

#ifdef HAVE_IPV6
	if (addrs->ai_family == AF_INET && addrs->ai_next != NULL && addrs->ai_next->ai_family == AF_INET6)
	{
		/*
		 * We got both an INET and INET6 possibility, but we want to prefer
		 * the INET6 one if it works. Reverse the order we got from
		 * getaddrinfo so that we try things in our preferred order. If we got
		 * more possibilities (other AFs??), I don't think we care about them,
		 * so don't worry if the list is more that two, we just rearrange the
		 * first two.
		 */
		struct addrinfo *temp = addrs->ai_next; /* second node */

		addrs->ai_next = addrs->ai_next->ai_next;	/* point old first node to
													 * third node if any */
		temp->ai_next = addrs;	/* point second node to first */
		addrs = temp;			/* start the list with the old second node */
	}
#endif

	for (rp = addrs; rp != NULL; rp = rp->ai_next)
	{
		/*
		 * getaddrinfo gives us all the parameters for the socket() call as
		 * well as the parameters for the bind() call.
		 */

		fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (fd == -1)
			continue;

		/*
		 * we let the system pick the TCP port here so we don't have to manage
		 * port resources ourselves.
		 */

		if (bind(fd, rp->ai_addr, rp->ai_addrlen) == 0)
			break;				/* Success */

		close(fd);
		fd = -1;
	}

	fun = "bind";
	if (fd == -1)
		goto error;

	/* Make socket non-blocking. */
	fun = "fcntl(O_NONBLOCK)";
	if (!pg_set_noblock(fd))
		goto error;

	fun = "listen";
	if (listen(fd, backlog) < 0)
		goto error;

	/* Get the listening socket's port number. */
	fun = "getsockname";
	addrlen = sizeof(addr);
	if (getsockname(fd, (struct sockaddr *) &addr, &addrlen) < 0)
		goto error;

	/* Give results to caller. */
	*listenerSocketFd = fd;

	/* display which port was chosen by the system. */
	if (addr.ss_family == AF_INET6)
		*listenerPort = ntohs(((struct sockaddr_in6 *) &addr)->sin6_port);
	else
		*listenerPort = ntohs(((struct sockaddr_in *) &addr)->sin_port);

	freeaddrinfo(addrs);
	return;

error:
	errnoSave = errno;
	if (fd >= 0)
		closesocket(fd);
	errno = errnoSave;
	freeaddrinfo(addrs);
	ereport(ERROR,
			(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
			 errmsg("interconnect Error: Could not set up tcp listener socket"),
			 errdetail("%s: %m", fun)));
}								/* setupListeningSocket */

/*
 * Initialize TCP specific comms.
 */
void
InitMotionTCP(int *listenerSocketFd, int32 *listenerPort)
{
	tval.tv_sec = 0;
	tval.tv_usec = 500000;

	setupTCPListeningSocket(listenerBacklog, listenerSocketFd, listenerPort);

	return;
}

void
InitMotionIPCLayerTCP(void)
{
	InitMotionTCP(&TCP_listenerFd, &tcp_listener_port);

	elog(DEBUG1, "Interconnect listening on tcp port %d ", tcp_listener_port);
}

void
CleanUpMotionIPCLayerTCP(void)
{
	if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
		elog(DEBUG3, "Cleaning Up Motion Layer IPC...");

	/* close down the Interconnect listener socket. */
	if (TCP_listenerFd >= 0)
		closesocket(TCP_listenerFd);

	/* be safe and reset global state variables. */
	tcp_listener_port = 0;
	TCP_listenerFd = -1;
}

/* Function readPacket() is used to read in the next packet from the given
 * MotionConn.
 *
 * This call blocks until the packet is read in, and is part of a
 * global scheme where senders block until the entire message is sent, and
 * receivers block until the entire message is read.  Both use non-blocking
 * socket calls so that we can handle any PG interrupts.
 *
 * Note, that for speed we want to read a message all in one go,
 * header and all. A consequence is that we may read in part of the
 * next message, which we've got to keep track of ... recvBytes holds
 * the byte-count of the unprocessed messages.
 *
 * PARAMETERS
 *	 conn - MotionConn to read the packet from.
 *
 */
static inline void
readPacket(MotionConn * conn, ChunkTransportState * transportStates)
{
	int			n,
				bytesRead = conn->recvBytes;
	bool		gotHeader = false,
				gotPacket = false;
	mpp_fd_set	rset;

#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG5, "readpacket: (fd %d) (max %d) outstanding bytes %d", conn->sockfd, Gp_max_packet_size, conn->recvBytes);
#endif

	/* do we have a complete message waiting to be processed ? */
	if (conn->recvBytes >= PACKET_HEADER_SIZE)
	{
		memcpy(&conn->msgSize, conn->msgPos, sizeof(uint32));
		gotHeader = true;
		if (conn->recvBytes >= conn->msgSize)
		{
#ifdef AMS_VERBOSE_LOGGING
			elog(DEBUG5, "readpacket: returning previously read data (%d)", conn->recvBytes);
#endif
			return;
		}
	}

	/*
	 * partial message waiting in recv buffer! Move to head of buffer:
	 * eliminate the slack (which will always be at the beginning) in the
	 * buffer
	 */
	if (conn->recvBytes != 0)
		memmove(conn->pBuff, conn->msgPos, conn->recvBytes);

	conn->msgPos = conn->pBuff;

#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG5, "readpacket: %s on previous call msgSize %d", gotHeader ? "got header" : "no header", conn->msgSize);
#endif

	while (!gotPacket && bytesRead < Gp_max_packet_size)
	{
		/* see if user canceled and stuff like that */
		ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);

		/*
		 * we read at the end of the buffer, we've eliminated any slack above
		 */
		if ((n = recv(conn->sockfd, conn->pBuff + bytesRead,
					  Gp_max_packet_size - bytesRead, 0)) < 0)
		{
			if (errno == EINTR)
				continue;
			if (errno == EWOULDBLOCK)
			{
				int			retry = 0;

				do
				{
					struct timeval timeout = tval;

					/* check for the QD cancel for every 2 seconds */
					if (retry++ > 4)
					{
						retry = 0;

						/* check to see if the dispatcher should cancel */
						if (Gp_role == GP_ROLE_DISPATCH)
						{
							checkForCancelFromQD(transportStates);
						}

					}

					/* see if user canceled and stuff like that */
					ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);

					MPP_FD_ZERO(&rset);
					MPP_FD_SET(conn->sockfd, &rset);
					n = select(conn->sockfd + 1, (fd_set *) &rset, NULL, NULL, &timeout);
					if (n == 0 || (n < 0 && errno == EINTR))
						continue;
					else if (n < 0)
					{
						ereport(ERROR,
								(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
								 errmsg("interconnect error reading an incoming packet"),
								 errdetail("select from seg%d at %s: %m",
										   conn->remoteContentId,
										   conn->remoteHostAndPort)));
					}
				}
				while (n < 1);
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						 errmsg("interconnect error reading an incoming packet"),
						 errdetail("read from seg%d at %s: %m",
								   conn->remoteContentId,
								   conn->remoteHostAndPort)));
			}
		}
		else if (n == 0)
		{
#ifdef AMS_VERBOSE_LOGGING
			elog(DEBUG5, "readpacket(); breaking in while (fd %d) recvBytes %d msgSize %d", conn->sockfd, conn->recvBytes, conn->msgSize);
			print_connection(transportStates, conn->sockfd, "interconnect error on");
#endif
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("interconnect error: connection closed prematurely"),
					 errdetail("from Remote Connection: contentId=%d at %s",
							   conn->remoteContentId, conn->remoteHostAndPort)));
			break;
		}
		else
		{
			bytesRead += n;

			if (!gotHeader && bytesRead >= PACKET_HEADER_SIZE)
			{
				/* got the header */
				memcpy(&conn->msgSize, conn->msgPos, sizeof(uint32));
				gotHeader = true;
			}
			conn->recvBytes = bytesRead;

			if (gotHeader && bytesRead >= conn->msgSize)
				gotPacket = true;
		}
	}
#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG5, "readpacket: got %d bytes", conn->recvBytes);
#endif
}

static void
flushIncomingData(int fd)
{
	static char trash[8192];
	int			bytes;

	/*
	 * If we're in TeardownInterconnect, we should only have to call recv() a
	 * couple of times to empty out our socket buffers
	 */
	do
	{
		bytes = recv(fd, trash, sizeof(trash), 0);
	} while (bytes > 0);
}

/* Function startOutgoingConnections() is used to initially kick-off any outgoing
 * connections for mySlice.
 *
 * This should not be called for root slices (i.e. QD ones) since they don't
 * ever have outgoing connections.
 *
 * PARAMETERS
 *
 *  sendSlice - Slice that this process is member of.
 *  pIncIdx - index in the parent slice list of myslice.
 *
 * RETURNS
 *	 Initialized ChunkTransportState for the Sending Motion Node Id.
 */
static ChunkTransportStateEntry *
startOutgoingConnections(ChunkTransportState * transportStates,
						 ExecSlice * sendSlice,
						 int *pOutgoingCount)
{
	ChunkTransportStateEntry *pEntry = NULL;
	MotionConn *conn = NULL;
	ListCell   *cell;
	ExecSlice  *recvSlice;
	CdbProcess *cdbProc;
	size_t		conn_index = 0;

	*pOutgoingCount = 0;

	recvSlice = &transportStates->sliceTable->slices[sendSlice->parentIndex];

	if (gp_interconnect_aggressive_retry)
	{
		if ((list_length(recvSlice->children) * list_length(sendSlice->segments)) > listenerBacklog)
			transportStates->aggressiveRetry = true;
	}
	else
		transportStates->aggressiveRetry = false;

	if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
		elog(DEBUG4, "Interconnect seg%d slice%d setting up sending motion node (aggressive retry is %s)",
			 GpIdentity.segindex, sendSlice->sliceIndex,
			 (transportStates->aggressiveRetry ? "active" : "inactive"));

	pEntry = createChunkTransportState(transportStates,
									   sendSlice,
									   recvSlice,
									   list_length(recvSlice->primaryProcesses),
									   sizeof(ChunkTransportStateEntryTCP));

	/*
	 * Setup a MotionConn entry for each of our outbound connections. Request
	 * a connection to each receiving backend's listening port.
	 */

	foreach(cell, recvSlice->primaryProcesses)
	{
		cdbProc = (CdbProcess *) lfirst(cell);
		if (cdbProc)
		{
			getMotionConn(pEntry, conn_index, &conn);
			conn->cdbProc = cdbProc;
			conn->pBuff = palloc(Gp_max_packet_size);
			conn->state = mcsSetupOutgoingConnection;
			(*pOutgoingCount)++;
		}
		conn_index++;
	}

	return pEntry;
}								/* startOutgoingConnections */


/*
 * setupOutgoingConnection
 *
 * Called by SetupTCPInterconnect when conn->state == mcsSetupOutgoingConnection.
 *
 * On return, state is:
 *      mcsSetupOutgoingConnection if failed and caller should retry.
 *      mcsConnecting if non-blocking connect() is pending.  Caller should
 *          send registration message when socket becomes write-ready.
 *      mcsSendRegMsg or mcsStarted if connect() completed successfully.
 */
static void
setupOutgoingConnection(ChunkTransportState * transportStates, ChunkTransportStateEntry * pEntry, MotionConn * conn)
{
	CdbProcess *cdbProc = NULL;
	MotionConnTCP *tcp_conn = NULL;

	int			n;

	int			ret;
	char		portNumberStr[32];
	char	   *service;
	struct addrinfo *addrs = NULL;
	struct addrinfo hint;

	tcp_conn = CONTAINER_OF(conn, MotionConnTCP, mConn);
	Assert(conn->cdbProc);
	Assert(conn->state == mcsSetupOutgoingConnection);
	cdbProc = conn->cdbProc;
	tcp_conn->wakeup_ms = 0;
	conn->remoteContentId = cdbProc->contentid;

	/*
	 * record the destination IP addr and port for error messages. Since the
	 * IP addr might be IPv6, it might have ':' embedded, so in that case, put
	 * '[]' around it so we can see that the string is an IP and port
	 * (otherwise it might look just like an IP).
	 */
	if (strchr(cdbProc->listenerAddr, ':') != 0)
		snprintf(conn->remoteHostAndPort, sizeof(conn->remoteHostAndPort),
				 "[%s]:%d", cdbProc->listenerAddr, cdbProc->listenerPort);
	else
		snprintf(conn->remoteHostAndPort, sizeof(conn->remoteHostAndPort),
				 "%s:%d", cdbProc->listenerAddr, cdbProc->listenerPort);

	/* Might be retrying due to connection failure etc.  Close old socket. */
	if (conn->sockfd >= 0)
	{
		closesocket(conn->sockfd);
		conn->sockfd = -1;
	}

#ifdef ENABLE_IC_PROXY
	if (CurrentMotionIPCLayer->ic_type == INTERCONNECT_TYPE_PROXY)
	{
		/*
		 * Using libuv pipe to register backend to proxy.
		 * ic_proxy_backend_connect only appends the connect request into
		 * connection queue and waits for the libuv_run_loop to handle the
		 * queue.
		 */
		ic_proxy_backend_connect(transportStates->proxyContext,
								 pEntry, conn, true);

		conn->pBuff = palloc(Gp_max_packet_size);
		conn->recvBytes = 0;
		conn->msgPos = NULL;
		conn->msgSize = PACKET_HEADER_SIZE;

		conn->state = mcsStarted;
		conn->stillActive = true;
		conn->tupleCount = 0;
		conn->remoteContentId = conn->cdbProc->contentid;
		return;
	}
#endif							/* ENABLE_IC_PROXY */

	/* Initialize hint structure */
	MemSet(&hint, 0, sizeof(hint));
	hint.ai_socktype = SOCK_STREAM;
	hint.ai_family = AF_UNSPEC; /* Allow for IPv4 or IPv6  */
#ifdef AI_NUMERICSERV
	hint.ai_flags = AI_NUMERICHOST | AI_NUMERICSERV;	/* Never do name
														 * resolution */
#else
	hint.ai_flags = AI_NUMERICHOST; /* Never do name resolution */
#endif

	snprintf(portNumberStr, sizeof(portNumberStr), "%d", cdbProc->listenerPort);
	service = portNumberStr;

	ret = pg_getaddrinfo_all(cdbProc->listenerAddr, service, &hint, &addrs);
	if (ret || !addrs)
	{
		if (addrs)
			pg_freeaddrinfo_all(hint.ai_family, addrs);

		ereport(ERROR,
				(errmsg("could not translate host addr \"%s\", port \"%d\" to address: %s",
						cdbProc->listenerAddr, cdbProc->listenerPort, gai_strerror(ret))));

		return;
	}

	/*
	 * Since we aren't using name resolution, getaddrinfo will return only 1
	 * entry
	 */

	/*
	 * Create a socket.  getaddrinfo() returns the parameters needed by
	 * socket()
	 */
	conn->sockfd = socket(addrs->ai_family, addrs->ai_socktype, addrs->ai_protocol);
	if (conn->sockfd < 0)
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("interconnect error setting up outgoing connection"),
				 errdetail("%s: %m", "socket")));

	/* make socket non-blocking BEFORE we connect. */
	if (!pg_set_noblock(conn->sockfd))
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("interconnect error setting up outgoing connection"),
				 errdetail("%s: %m", "fcntl(O_NONBLOCK)")));

	if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
		ereport(DEBUG1, (errmsg("Interconnect connecting to seg%d slice%d %s "
								"pid=%d sockfd=%d",
								conn->remoteContentId,
								pEntry->recvSlice->sliceIndex,
								conn->remoteHostAndPort,
								conn->cdbProc->pid,
								conn->sockfd)));

	/*
	 * Initiate the connection.
	 */
	for (;;)
	{							/* connect() EINTR retry loop */
		ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);

		n = connect(conn->sockfd, addrs->ai_addr, addrs->ai_addrlen);

		/* Non-blocking socket never connects immediately, but check anyway. */
		if (n == 0)
		{
			sendRegisterMessage(transportStates, pEntry, conn);
			pg_freeaddrinfo_all(hint.ai_family, addrs);
			return;
		}

		/* Retry if a signal was received. */
		if (errno == EINTR)
			continue;

		/* Normal case: select() will tell us when connection is made. */
		if (errno == EINPROGRESS ||
			errno == EWOULDBLOCK)
		{
			conn->state = mcsConnecting;
			pg_freeaddrinfo_all(hint.ai_family, addrs);
			return;
		}

		pg_freeaddrinfo_all(hint.ai_family, addrs);
		/* connect() failed.  Log the error.  Caller should retry. */
		updateOutgoingConnection(transportStates, pEntry, conn, errno);
		return;
	}							/* connect() EINTR retry loop */
}								/* setupOutgoingConnection */


/*
 * updateOutgoingConnection
 *
 * Called when connect() succeeds or fails.
 */
static void
updateOutgoingConnection(ChunkTransportState * transportStates, ChunkTransportStateEntry * pEntry, MotionConn * conn, int errnoSave)
{
	socklen_t	sizeoferrno = sizeof(errnoSave);

	/* Get errno value indicating success or failure. */
	if (errnoSave == -1 &&
		getsockopt(conn->sockfd, SOL_SOCKET, SO_ERROR,
				   (void *) &errnoSave, &sizeoferrno))
	{
		/* getsockopt failed */
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("interconnect could not connect to seg%d %s",
						conn->remoteContentId, conn->remoteHostAndPort),
				 errdetail("%s sockfd=%d: %m",
						   "getsockopt(SO_ERROR)", conn->sockfd)));
	}

	switch (errnoSave)
	{
			/* Success!  Advance to next state. */
		case 0:
			sendRegisterMessage(transportStates, pEntry, conn);
			return;
		default:
			errno = errnoSave;
			ereport(LOG,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("interconnect could not connect to seg%d %s pid=%d; will retry; %s: %m",
							conn->remoteContentId, conn->remoteHostAndPort,
							conn->cdbProc->pid, "connect")));
			break;
	}

	/* Tell caller to close the socket and try again. */
	conn->state = mcsSetupOutgoingConnection;
}								/* updateOutgoingConnection */

/* Function sendRegisterMessage() used to send a Register message to the
 * remote destination on the other end of the provided conn.
 *
 * PARAMETERS
 *
 *	 pEntry - ChunkTransportState.
 *	 conn	- MotionConn to send message out on.
 *
 * Called by SetupTCPInterconnect when conn->state == mcsSetupOutgoingConnection.
 *
 * On return, state is:
 *      mcsSendRegMsg if registration message has not been completely sent.
 *          Caller should retry when socket becomes write-ready.
 *      mcsStarted if registration message has been sent.  Caller can start
 *          sending data.
 */
static void
sendRegisterMessage(ChunkTransportState * transportStates, ChunkTransportStateEntry * pEntry, MotionConn * conn)
{
	int			bytesToSend;
	int			bytesSent;
	MotionConnTCP *tcp_conn = NULL;
	SliceTable *sliceTbl = transportStates->sliceTable;

	tcp_conn = CONTAINER_OF(conn, MotionConnTCP, mConn);

	if (conn->state != mcsSendRegMsg)
	{
		RegisterMessage *regMsg = (RegisterMessage *) conn->pBuff;
		struct sockaddr_storage localAddr;
		socklen_t	addrsize;

		Assert(conn->cdbProc &&
			   conn->pBuff &&
			   sizeof(*regMsg) <= Gp_max_packet_size);

		/* Save local host and port for log messages. */
		addrsize = sizeof(localAddr);
		if (getsockname(conn->sockfd, (struct sockaddr *) &localAddr, &addrsize))
		{
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("interconnect error after making connection"),
					 errdetail("getsockname sockfd=%d remote=%s: %m",
							   conn->sockfd, conn->remoteHostAndPort)));
		}
		format_sockaddr(&localAddr, tcp_conn->localHostAndPort,
						sizeof(tcp_conn->localHostAndPort));

		if (gp_log_interconnect >= GPVARS_VERBOSITY_VERBOSE)
			ereport(LOG,
					(errmsg("interconnect sending registration message to seg%d slice%d %s pid=%d from seg%d slice%d %s sockfd=%d",
							conn->remoteContentId,
							pEntry->recvSlice->sliceIndex,
							conn->remoteHostAndPort,
							conn->cdbProc->pid,
							GpIdentity.segindex,
							pEntry->sendSlice->sliceIndex,
							tcp_conn->localHostAndPort,
							conn->sockfd)));

		regMsg->msgBytes = sizeof(*regMsg);
		regMsg->recvSliceIndex = pEntry->recvSlice->sliceIndex;
		regMsg->sendSliceIndex = pEntry->sendSlice->sliceIndex;

		regMsg->srcContentId = GpIdentity.segindex;
		regMsg->srcListenerPort = GetListenPortTCP();
		regMsg->srcPid = MyProcPid;
		regMsg->srcSessionId = gp_session_id;
		regMsg->srcCommandCount = sliceTbl->ic_instance_id;


		conn->state = mcsSendRegMsg;
		conn->msgPos = conn->pBuff;
		conn->msgSize = sizeof(*regMsg);
	}

	/* Send as much as we can. */
	for (;;)
	{
		bytesToSend = conn->pBuff + conn->msgSize - conn->msgPos;
		bytesSent = send(conn->sockfd, conn->msgPos, bytesToSend, 0);
		if (bytesSent == bytesToSend)
			break;
		else if (bytesSent >= 0)
			conn->msgPos += bytesSent;
		else if (errno == EWOULDBLOCK)
			return;				/* call me again to send the rest */
		else if (errno == EINTR)
			ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("interconnect error writing registration message to seg%d at %s",
							conn->remoteContentId,
							conn->remoteHostAndPort),
					 errdetail("write pid=%d sockfd=%d local=%s: %m",
							   conn->cdbProc->pid,
							   conn->sockfd,
							   tcp_conn->localHostAndPort)));
		}
	}

	/* Sent it all. */
	conn->state = mcsStarted;
	conn->msgPos = NULL;
	conn->msgSize = PACKET_HEADER_SIZE;
	conn->stillActive = true;
}								/* sendRegisterMessage */


/* Function readRegisterMessage() reads a "Register" message off of the conn
 * and places it in the right MotionLayerEntry conn slot based on the contents
 * of the register message.
 *
 * PARAMETERS
 *
 *	 conn - MotionConn to read the register messagefrom.
 *
 * Returns true if message has been received; or false if caller must retry
 * when socket becomes read-ready.
 */
static bool
readRegisterMessage(ChunkTransportState * transportStates,
					MotionConn * conn)
{
	int			bytesToReceive;
	int			bytesReceived;
	int			iconn;
	RegisterMessage *regMsg;
	RegisterMessage msg;
	MotionConn *newConn = NULL;
	ChunkTransportStateEntry *pChunkEntry = NULL;
	ChunkTransportStateEntryTCP *pEntry = NULL;
	CdbProcess *cdbproc = NULL;
	ListCell   *lc;
	SliceTable *sliceTbl = transportStates->sliceTable;
	MotionConnTCP *tcp_conn = NULL;

	tcp_conn = CONTAINER_OF(conn, MotionConnTCP, mConn);

	/* Get ready to receive the Register message. */
	if (conn->state != mcsRecvRegMsg)
	{
		conn->state = mcsRecvRegMsg;
		conn->msgSize = sizeof(*regMsg);
		conn->msgPos = conn->pBuff;

		Assert(conn->pBuff &&
			   sizeof(*regMsg) <= Gp_max_packet_size);
	}

	/* Receive all that is available, up to the expected message size. */
	for (;;)
	{
		bytesToReceive = conn->pBuff + conn->msgSize - conn->msgPos;
		bytesReceived = recv(conn->sockfd, conn->msgPos, bytesToReceive, 0);
		if (bytesReceived == bytesToReceive)
			break;
		else if (bytesReceived > 0)
			conn->msgPos += bytesReceived;
		else if (bytesReceived == 0)
		{
			elog(LOG, "Interconnect error reading register message from %s: connection closed",
				 conn->remoteHostAndPort);

			/* maybe this peer is already retrying ? */
			goto old_conn;
		}
		else if (errno == EWOULDBLOCK)
			return false;		/* call me again to receive the rest */
		else if (errno == EINTR)
			ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("interconnect error reading register message from %s",
							conn->remoteHostAndPort),
					 errdetail("read sockfd=%d local=%s: %m",
							   conn->sockfd,
							   tcp_conn->localHostAndPort)));
		}
	}

	/*
	 * Got the whole message.  Convert fields to native byte order.
	 */
	regMsg = (RegisterMessage *) conn->pBuff;
	msg.msgBytes = regMsg->msgBytes;
	msg.recvSliceIndex = regMsg->recvSliceIndex;
	msg.sendSliceIndex = regMsg->sendSliceIndex;

	msg.srcContentId = regMsg->srcContentId;
	msg.srcListenerPort = regMsg->srcListenerPort;
	msg.srcPid = regMsg->srcPid;
	msg.srcSessionId = regMsg->srcSessionId;
	msg.srcCommandCount = regMsg->srcCommandCount;

	/* Check for valid message format. */
	if (msg.msgBytes != sizeof(*regMsg))
	{
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("interconnect error reading register message from %s: format not recognized",
						conn->remoteHostAndPort),
				 errdetail("msgBytes=%d expected=%d sockfd=%d local=%s",
						   msg.msgBytes, (int) sizeof(*regMsg),
						   conn->sockfd, tcp_conn->localHostAndPort)));
	}

	/* get rid of old connections first */
	if (msg.srcSessionId != gp_session_id ||
		msg.srcCommandCount < sliceTbl->ic_instance_id)
	{
		/*
		 * This is an old connection, which can be safely ignored. We get this
		 * kind of stuff for cases in which one gang participating in the
		 * interconnect exited a query before calling SetupTCPInterconnect().
		 * Later queries wind up receiving their registration messages.
		 */
		elog(LOG, "Received invalid, old registration message: "
			 "will ignore ('expected:received' session %d:%d ic-id %d:%d)",
			 gp_session_id, msg.srcSessionId,
			 sliceTbl->ic_instance_id, msg.srcCommandCount);

		goto old_conn;
	}

	/* Verify that the message pertains to one of our receiving Motion nodes. */
	if (msg.sendSliceIndex > 0 &&
		msg.sendSliceIndex <= transportStates->size &&
		msg.recvSliceIndex == transportStates->sliceId &&
		msg.srcContentId >= -1)
	{
		/* this is a good connection */
	}
	else
	{
		/* something is wrong */
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("interconnect error: Invalid registration message received from %s",
						conn->remoteHostAndPort),
				 errdetail("sendSlice=%d recvSlice=%d srcContentId=%d srcPid=%d srcListenerPort=%d srcSessionId=%d srcCommandCount=%d motnode=%d",
						   msg.sendSliceIndex, msg.recvSliceIndex,
						   msg.srcContentId, msg.srcPid,
						   msg.srcListenerPort, msg.srcSessionId,
						   msg.srcCommandCount, msg.sendSliceIndex)));
	}

	/*
	 * Find state info for the specified Motion node.  The sender's slice
	 * number equals the motion node id.
	 */
	getChunkTransportState(transportStates, msg.sendSliceIndex, &pChunkEntry);
	pEntry = CONTAINER_OF(pChunkEntry, ChunkTransportStateEntryTCP, entry);
	Assert(pEntry);

	foreach_with_count(lc, pEntry->entry.sendSlice->primaryProcesses, iconn)
	{
		cdbproc = (CdbProcess *) lfirst(lc);

		if (!cdbproc)
			continue;

		if (msg.srcContentId == cdbproc->contentid &&
			msg.srcListenerPort == cdbproc->listenerPort &&
			msg.srcPid == cdbproc->pid)
			break;
	}

	if (iconn == list_length(pEntry->entry.sendSlice->primaryProcesses))
	{
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("interconnect error: Invalid registration message received from %s",
						conn->remoteHostAndPort),
				 errdetail("sendSlice=%d srcContentId=%d srcPid=%d srcListenerPort=%d",
						   msg.sendSliceIndex, msg.srcContentId,
						   msg.srcPid, msg.srcListenerPort)));
	}

	/*
	 * Allocate MotionConn slot corresponding to sender's position in the
	 * sending slice's CdbProc list.
	 */
	getMotionConn(pChunkEntry, iconn, &newConn);

	if (newConn->sockfd != -1 ||
		newConn->state != mcsNull)
	{
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("interconnect error: Duplicate registration message received from %s",
						conn->remoteHostAndPort),
				 errdetail("Already accepted registration from %s for sendSlice=%d srcContentId=%d srcPid=%d srcListenerPort=%d",
						   newConn->remoteHostAndPort, msg.sendSliceIndex,
						   msg.srcContentId, msg.srcPid, msg.srcListenerPort)));
	}

	/* message looks good */
	if (gp_log_interconnect >= GPVARS_VERBOSITY_VERBOSE)
	{
		ereport(LOG,
				(errmsg("interconnect seg%d slice%d sockfd=%d accepted registration message from seg%d slice%d %s pid=%d",
						GpIdentity.segindex, msg.recvSliceIndex, conn->sockfd,
						msg.srcContentId, msg.sendSliceIndex,
						conn->remoteHostAndPort, msg.srcPid)));
	}

	/* Copy caller's temporary MotionConn to its assigned slot. */
	*newConn = *conn;

	newConn->cdbProc = cdbproc;
	newConn->remoteContentId = msg.srcContentId;

	/*
	 * The caller's MotionConn object is no longer valid.
	 */
	MemSet(conn, 0, sizeof(*conn));
	conn->state = mcsNull;

	/*
	 * Prepare to begin reading tuples.
	 */
	newConn->state = mcsStarted;
	newConn->msgPos = NULL;
	newConn->msgSize = 0;
	newConn->stillActive = true;

	MPP_FD_SET(newConn->sockfd, &pEntry->entry.readSet);

	if (newConn->sockfd > pEntry->highReadSock)
		pEntry->highReadSock = newConn->sockfd;

#ifdef AMS_VERBOSE_LOGGING
	dumpEntryConnections(DEBUG4, &pEntry->entry);
#endif

	/* we've completed registration of this connection */
	return true;

old_conn:
	shutdown(conn->sockfd, SHUT_RDWR);
	closesocket(conn->sockfd);
	conn->sockfd = -1;

	pfree(conn->pBuff);
	conn->pBuff = NULL;

	/*
	 * this connection is done, but with sockfd == -1 isn't a "success"
	 */
	return true;
}								/* readRegisterMessage */


/*
 * acceptIncomingConnection
 *
 * accept() a connection request that is pending on the listening socket.
 * Returns a newly palloc'ed MotionConn object; or NULL if the listening
 * socket does not have any pending connection requests.
 */
static MotionConn *
acceptIncomingConnection(void)
{
	int			newsockfd;
	socklen_t	addrsize;
	MotionConn *conn = NULL;
	struct sockaddr_storage remoteAddr;
	struct sockaddr_storage localAddr;
	MotionConnTCP *tcp_conn = NULL;

	/*
	 * Accept a connection.
	 */
	for (;;)
	{							/* loop until success or EWOULDBLOCK */
		MemSet(&remoteAddr, 0, sizeof(remoteAddr));
		addrsize = sizeof(remoteAddr);
		newsockfd = accept(TCP_listenerFd, (struct sockaddr *) &remoteAddr, &addrsize);
		if (newsockfd >= 0)
			break;

		switch (errno)
		{
			case EINTR:
				/* A signal arrived.  Loop to retry the accept(). */
				break;

			case EWOULDBLOCK:
				/* Connection request queue is empty.  Normal return. */
				return NULL;

			case EBADF:
			case EFAULT:
			case EINVAL:
#ifndef _WIN32
			case ENOTSOCK:
#endif
			case EOPNOTSUPP:
				/* Shouldn't get these errors unless there is a bug. */
				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						 errmsg("interconnect error on listener port %d",
								tcp_listener_port),
						 errdetail("accept sockfd=%d: %m", TCP_listenerFd)));
				break;			/* not reached */
			case ENOMEM:
			case ENFILE:
			case EMFILE:
			case ENOBUFS:
				/* Out of resources. */
				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						 errmsg("interconnect error on listener port %d",
								tcp_listener_port),
						 errdetail("accept sockfd=%d: %m", TCP_listenerFd)));
				break;			/* not reached */
			default:
				/* Network problem, connection aborted, etc.  Continue. */
				ereport(LOG,
						(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						 errmsg("interconnect connection request not completed on listener port %d",
								tcp_listener_port),
						 errdetail("accept sockfd=%d: %m", TCP_listenerFd)));
		}						/* switch (errno) */
	}							/* loop until success or EWOULDBLOCK */

	/*
	 * Create a MotionConn object to hold the connection state.
	 */
	conn = palloc0(sizeof(MotionConnTCP));
	conn->sockfd = newsockfd;
	conn->pBuff = palloc(Gp_max_packet_size);
	conn->msgSize = 0;
	conn->recvBytes = 0;
	conn->msgPos = 0;
	conn->tupleCount = 0;
	conn->stillActive = false;
	conn->state = mcsAccepted;
	conn->remoteContentId = -2;

	tcp_conn = CONTAINER_OF(conn, MotionConnTCP, mConn);

	/* Save remote and local host:port strings for error messages. */
	format_sockaddr(&remoteAddr, conn->remoteHostAndPort,
					sizeof(conn->remoteHostAndPort));
	addrsize = sizeof(localAddr);
	if (getsockname(newsockfd, (struct sockaddr *) &localAddr, &addrsize))
	{
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("interconnect error after accepting connection"),
				 errdetail("getsockname sockfd=%d remote=%s: %m",
						   newsockfd, conn->remoteHostAndPort)));
	}
	format_sockaddr(&localAddr, tcp_conn->localHostAndPort,
					sizeof(tcp_conn->localHostAndPort));

	/* make socket non-blocking */
	if (!pg_set_noblock(newsockfd))
	{
		ereport(ERROR,
				(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
				 errmsg("interconnect error after accepting connection"),
				 errdetail("fcntl(O_NONBLOCK) sockfd=%d remote=%s local=%s: %m",
						   newsockfd, conn->remoteHostAndPort,
						   tcp_conn->localHostAndPort)));
	}

	if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
		elog(DEBUG4, "Interconnect got incoming connection "
			 "from remote=%s to local=%s sockfd=%d",
			 conn->remoteHostAndPort, tcp_conn->localHostAndPort, newsockfd);

	return conn;
}								/* acceptIncomingConnection */

static inline void
SetupTCPInterconnect(EState *estate)
{
	int			i,
				index,
				n;
	ListCell   *cell;
	ExecSlice  *mySlice;
	ExecSlice  *aSlice;
	MotionConn *conn = NULL;
	MotionConnTCP *tcp_conn = NULL;
	SliceTable *sliceTable = estate->es_sliceTable;
	int			incoming_count = 0;
	int			outgoing_count = 0;
	int			expectedTotalIncoming = 0;
	int			expectedTotalOutgoing = 0;
	int			iteration = 0;
	GpMonotonicTime startTime;
	StringInfoData logbuf;
	uint64		elapsed_ms = 0;
	uint64		last_qd_check_ms = 0;

	/* we can have at most one of these. */
	ChunkTransportStateEntry *sendingChunkTransportState = NULL;
	ChunkTransportState *interconnect_context;
	HASHCTL		conn_sent_record_typmod_ctl;

	SIMPLE_FAULT_INJECTOR("interconnect_setup_palloc");
	interconnect_context = palloc0(sizeof(ChunkTransportState));

	/* initialize state variables */
	interconnect_context->estate = estate;
	interconnect_context->size = CTS_INITIAL_SIZE;
	interconnect_context->states = palloc0(CTS_INITIAL_SIZE * sizeof(ChunkTransportStateEntryTCP));

	interconnect_context->teardownActive = false;
	interconnect_context->activated = false;
	interconnect_context->networkTimeoutIsLogged = false;
	interconnect_context->incompleteConns = NIL;
	interconnect_context->sliceTable = copyObject(sliceTable);
	interconnect_context->sliceId = sliceTable->localSlice;

	conn_sent_record_typmod_ctl.keysize = sizeof(MotionConnKey);
	conn_sent_record_typmod_ctl.entrysize = sizeof(MotionConnSentRecordTypmodEnt);
	conn_sent_record_typmod_ctl.hcxt = CurrentMemoryContext;

	interconnect_context->conn_sent_record_typmod = hash_create(
																"MotionConn sent record typmod mapping", 128, &conn_sent_record_typmod_ctl, HASH_CONTEXT | HASH_ELEM | HASH_BLOBS);

#ifdef ENABLE_IC_PROXY
	ic_proxy_backend_init_context(interconnect_context);
#endif							/* ENABLE_IC_PROXY */

	mySlice = &interconnect_context->sliceTable->slices[sliceTable->localSlice];

	Assert(sliceTable &&
		   mySlice->sliceIndex == sliceTable->localSlice);

	gp_set_monotonic_begin_time(&startTime);

	/* now we'll do some setup for each of our Receiving Motion Nodes. */
	foreach(cell, mySlice->children)
	{
		int			totalNumProcs;
		int			childId = lfirst_int(cell);
		ChunkTransportStateEntry *pEntry = NULL;

#ifdef AMS_VERBOSE_LOGGING
		elog(DEBUG5, "Setting up RECEIVING motion node %d", childId);
#endif

		aSlice = &interconnect_context->sliceTable->slices[childId];

		/*
		 * If we're using directed-dispatch we have dummy primary-process
		 * entries, so we count the entries.
		 */
		totalNumProcs = list_length(aSlice->primaryProcesses);

		pEntry = createChunkTransportState(interconnect_context, aSlice, mySlice, totalNumProcs, sizeof(ChunkTransportStateEntryTCP));

		for (i = 0; i < totalNumProcs; i++)
		{
			CdbProcess *cdbProc;

			cdbProc = list_nth(aSlice->primaryProcesses, i);
			if (cdbProc)
				expectedTotalIncoming++;

#ifdef ENABLE_IC_PROXY
			if (CurrentMotionIPCLayer->ic_type == INTERCONNECT_TYPE_PROXY)
			{
				getMotionConn(pEntry, i, &conn);
				conn->cdbProc = list_nth(aSlice->primaryProcesses, i);

				if (conn->cdbProc)
				{
					incoming_count++;

					/*
					 * Using libuv pipe to register backend to proxy.
					 * ic_proxy_backend_connect only appends the connect
					 * request into connection queue and waits for the
					 * libuv_run_loop to handle the queue.
					 */
					ic_proxy_backend_connect(interconnect_context->proxyContext,
											 pEntry, conn, false /* isSender */ );

					conn->pBuff = palloc(Gp_max_packet_size);
					conn->recvBytes = 0;
					conn->msgPos = NULL;
					conn->msgSize = 0;

					conn->state = mcsStarted;
					conn->stillActive = true;
					conn->tupleCount = 0;
					conn->remoteContentId = conn->cdbProc->contentid;

					conn->remapper = CreateTupleRemapper();
				}
			}
#endif							/* ENABLE_IC_PROXY */
		}
	}

	/*
	 * Initiate outgoing connections.
	 *
	 * startOutgoingConnections() and createChunkTransportState() must not be
	 * called during the lifecycle of sendingChunkTransportState, they will
	 * repalloc() interconnect_context->states so sendingChunkTransportState
	 * points to invalid memory.
	 */
	if (mySlice->parentIndex != -1)
		sendingChunkTransportState = startOutgoingConnections(interconnect_context, mySlice, &expectedTotalOutgoing);

#ifdef ENABLE_IC_PROXY
	if (CurrentMotionIPCLayer->ic_type == INTERCONNECT_TYPE_PROXY)
	{
		for (i = 0; i < expectedTotalOutgoing; i++)
		{
			getMotionConn(sendingChunkTransportState, i, &conn);
			setupOutgoingConnection(interconnect_context,
									sendingChunkTransportState, conn);
		}
		outgoing_count = expectedTotalOutgoing;
	}

	/*
	 * Before ic_proxy_backend_run_loop, we have already gone though all the
	 * incoming and outgoing connections and append them into the connect
	 * queue. ic_proxy_backend_run_loop will trigger the uv_loop and begin to
	 * handle the connect event in parallel and asynchronous way.
	 *
	 * Note that the domain socket fds are binded to libuv pipe handle, but we
	 * still depends on ic_tcp code to send/recv interconnect data based on
	 * these fds and close these fds in teardown function. As a result, we
	 * should not touch the libuv pipe handles until ic_tcp close all the fds
	 * in teardown function. In future, we should retire the ic_tcp code in
	 * ic_proxy backend and use libuv to handle connection setup, data
	 * transfer and teardown in a unified way.
	 */
	ic_proxy_backend_run_loop(interconnect_context->proxyContext);
#endif							/* ENABLE_IC_PROXY */

	if (expectedTotalIncoming > listenerBacklog)
		ereport(WARNING, (errmsg("SetupTCPInterconnect: too many expected incoming connections(%d), Interconnect setup might possibly fail", expectedTotalIncoming),
						  errhint("Try enlarging the gp_interconnect_tcp_listener_backlog GUC value and OS net.core.somaxconn parameter")));

	if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
		ereport(DEBUG1, (errmsg("SetupTCPInterconnect will activate "
								"%d incoming, %d outgoing routes.  "
								"Listening on port=%d sockfd=%d.",
								expectedTotalIncoming, expectedTotalOutgoing,
								tcp_listener_port, TCP_listenerFd)));

	/*
	 * Loop until all connections are completed or time limit is exceeded.
	 */
	while (outgoing_count < expectedTotalOutgoing ||
		   incoming_count < expectedTotalIncoming)
	{							/* select() loop */
		struct timeval timeout;
		mpp_fd_set	rset,
					wset,
					eset;
		int			highsock = -1;
		uint64		timeout_ms = 20 * 60 * 1000;
		int			outgoing_fail_count = 0;
		int			select_errno;

		iteration++;

		MPP_FD_ZERO(&rset);
		MPP_FD_ZERO(&wset);
		MPP_FD_ZERO(&eset);

		/* Expecting any new inbound connections? */
		if (incoming_count < expectedTotalIncoming)
		{
			if (TCP_listenerFd < 0)
			{
				elog(FATAL, "SetupTCPInterconnect: bad listener");
			}

			MPP_FD_SET(TCP_listenerFd, &rset);
			highsock = TCP_listenerFd;
		}

		/* Inbound connections awaiting registration message */
		foreach(cell, interconnect_context->incompleteConns)
		{
			conn = (MotionConn *) lfirst(cell);

			if (conn->state != mcsRecvRegMsg || conn->sockfd < 0)
			{
				elog(FATAL, "SetupTCPInterconnect: incomplete connection bad state or bad fd");
			}

			MPP_FD_SET(conn->sockfd, &rset);
			highsock = Max(highsock, conn->sockfd);
		}

		/* Outgoing connections */
		outgoing_count = 0;
		n = sendingChunkTransportState ? sendingChunkTransportState->numConns : 0;

		for (i = 0; i < n; i++)
		{
			index = i;
			getMotionConn(sendingChunkTransportState, index, &conn);
			tcp_conn = CONTAINER_OF(conn, MotionConnTCP, mConn);

			/* Time to cancel incomplete connect() and retry? */
			if (conn->state == mcsConnecting &&
				tcp_conn->wakeup_ms > 0 &&
				tcp_conn->wakeup_ms <= elapsed_ms + 20)
			{
				ereport(LOG, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
							  errmsg("Interconnect timeout: Connection "
									 "to seg%d %s from local port %s was not "
									 "complete after " UINT64_FORMAT
									 "ms " UINT64_FORMAT " elapsed.  Will retry.",
									 conn->remoteContentId,
									 conn->remoteHostAndPort,
									 tcp_conn->localHostAndPort,
									 tcp_conn->wakeup_ms, (elapsed_ms + 20))));
				conn->state = mcsSetupOutgoingConnection;
			}

			/* Time to connect? */
			if (conn->state == mcsSetupOutgoingConnection &&
				tcp_conn->wakeup_ms <= elapsed_ms + 20)
			{
				setupOutgoingConnection(interconnect_context, sendingChunkTransportState, conn);
				switch (conn->state)
				{
					case mcsSetupOutgoingConnection:
						/* Retry failed connection after awhile. */
						tcp_conn->wakeup_ms = (iteration - 1) * 1000 + elapsed_ms;
						break;
					case mcsConnecting:
						/* Set time limit for connect() to complete. */
						if (interconnect_context->aggressiveRetry)
							tcp_conn->wakeup_ms = CONNECT_AGGRESSIVERETRY_MS + elapsed_ms;
						else
							tcp_conn->wakeup_ms = CONNECT_RETRY_MS + elapsed_ms;
						break;
					default:
						tcp_conn->wakeup_ms = 0;
						break;
				}
			}

			/* What events are we watching for? */
			switch (conn->state)
			{
				case mcsNull:
					break;
				case mcsSetupOutgoingConnection:
					outgoing_fail_count++;
					break;
				case mcsConnecting:
					if (conn->sockfd < 0)
					{
						elog(FATAL, "SetupTCPInterconnect: bad fd, mcsConnecting");
					}

					MPP_FD_SET(conn->sockfd, &wset);
					MPP_FD_SET(conn->sockfd, &eset);
					highsock = Max(highsock, conn->sockfd);
					break;
				case mcsSendRegMsg:
					if (conn->sockfd < 0)
					{
						elog(FATAL, "SetupTCPInterconnect: bad fd, mcsSendRegMsg");
					}
					MPP_FD_SET(conn->sockfd, &wset);
					highsock = Max(highsock, conn->sockfd);
					break;
				case mcsStarted:
					outgoing_count++;
					break;
				default:
					elog(FATAL, "SetupTCPInterconnect: bad connection state");
			}

			if (tcp_conn->wakeup_ms > 0)
				timeout_ms = Min(timeout_ms, tcp_conn->wakeup_ms - elapsed_ms);
		}						/* loop to set up outgoing connections */

		/* Break out of select() loop if completed all connections. */
		if (outgoing_count == expectedTotalOutgoing &&
			incoming_count == expectedTotalIncoming)
			break;

		/*
		 * Been here long?  Bail if gp_interconnect_setup_timeout exceeded.
		 */
		if (interconnect_setup_timeout > 0)
		{
			int			to = interconnect_setup_timeout * 1000;

			if (to <= elapsed_ms + 20)
				ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
								errmsg("Interconnect timeout: Unable to "
									   "complete setup of all connections "
									   "within time limit."),
								errdetail("Completed %d of %d incoming and "
										  "%d of %d outgoing connections.  "
										  "gp_interconnect_setup_timeout = %d "
										  "seconds.",
										  incoming_count, expectedTotalIncoming,
										  outgoing_count, expectedTotalOutgoing,
										  interconnect_setup_timeout)
								));
			/* don't wait for more than 500ms */
			timeout_ms = Min(500, Min(timeout_ms, to - elapsed_ms));
		}

		/* check if segments have errors already for every 2 seconds */
		if (Gp_role == GP_ROLE_DISPATCH && elapsed_ms - last_qd_check_ms > 2000)
		{
			last_qd_check_ms = elapsed_ms;
			checkForCancelFromQD(interconnect_context);
		}

		/*
		 * If no socket events to wait for, loop to retry after a pause.
		 */
		if (highsock < 0)
		{
			if (gp_log_interconnect >= GPVARS_VERBOSITY_VERBOSE &&
				(timeout_ms > 0 || iteration > 2))
				ereport(LOG, (errmsg("SetupTCPInterconnect+" UINT64_FORMAT
									 "ms:   pause " UINT64_FORMAT "ms   "
									 "outgoing_fail=%d iteration=%d",
									 elapsed_ms, timeout_ms,
									 outgoing_fail_count, iteration)
							  ));

			/* Shouldn't be in this loop unless we have some work to do. */
			if (outgoing_fail_count <= 0)
			{
				elog(FATAL, "SetupTCPInterconnect: invalid outgoing count");
			}

			/* Wait until earliest wakeup time or overall timeout. */
			if (timeout_ms > 0)
			{
				ML_CHECK_FOR_INTERRUPTS(interconnect_context->teardownActive);
				pg_usleep(timeout_ms * 1000);
				ML_CHECK_FOR_INTERRUPTS(interconnect_context->teardownActive);
			}

			/* Back to top of loop and look again. */
			elapsed_ms = gp_get_elapsed_ms(&startTime);
			continue;
		}

		/*
		 * Wait for socket events.
		 *
		 * In order to handle errors at intervals less than the full timeout
		 * length, we limit our select(2) wait to a maximum of 500ms.
		 */
		if (timeout_ms > 0)
		{
			timeout.tv_sec = timeout_ms / 1000; /* 0 */
			timeout.tv_usec = (timeout_ms - (timeout.tv_sec * 1000)) * 1000;
			Assert(timeout_ms == timeout.tv_sec * 1000 + timeout.tv_usec / 1000);
		}
		else
			timeout.tv_sec = timeout.tv_usec = 0;

		if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
		{
			initStringInfo(&logbuf);

			format_fd_set(&logbuf, highsock + 1, &rset, "r={", "} ");
			format_fd_set(&logbuf, highsock + 1, &wset, "w={", "} ");
			format_fd_set(&logbuf, highsock + 1, &eset, "e={", "}");

			elapsed_ms = gp_get_elapsed_ms(&startTime);

			ereport(DEBUG1, (errmsg("SetupTCPInterconnect+" UINT64_FORMAT
									"ms:   select()  "
									"Interest: %s.  timeout=" UINT64_FORMAT "ms "
									"outgoing_fail=%d iteration=%d",
									elapsed_ms, logbuf.data, timeout_ms,
									outgoing_fail_count, iteration)));
			pfree(logbuf.data);
			MemSet(&logbuf, 0, sizeof(logbuf));
		}

		ML_CHECK_FOR_INTERRUPTS(interconnect_context->teardownActive);
		n = select(highsock + 1, (fd_set *) &rset, (fd_set *) &wset, (fd_set *) &eset, &timeout);
		select_errno = errno;

		ML_CHECK_FOR_INTERRUPTS(interconnect_context->teardownActive);
		if (Gp_role == GP_ROLE_DISPATCH)
			checkForCancelFromQD(interconnect_context);

		elapsed_ms = gp_get_elapsed_ms(&startTime);

		/*
		 * Log the select() if requested.
		 */
		if (gp_log_interconnect >= GPVARS_VERBOSITY_VERBOSE)
		{
			if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG ||
				n != expectedTotalIncoming + expectedTotalOutgoing)
			{
				int			elevel = (n == expectedTotalIncoming + expectedTotalOutgoing)
				? DEBUG1 : LOG;

				initStringInfo(&logbuf);
				if (n > 0)
				{
					appendStringInfo(&logbuf, "result=%d  Ready: ", n);
					format_fd_set(&logbuf, highsock + 1, &rset, "r={", "} ");
					format_fd_set(&logbuf, highsock + 1, &wset, "w={", "} ");
					format_fd_set(&logbuf, highsock + 1, &eset, "e={", "}");
				}
				else
					appendStringInfoString(&logbuf, n < 0 ? "error" : "timeout");
				ereport(elevel, (errmsg("SetupTCPInterconnect+" UINT64_FORMAT "ms:   select()  %s",
										elapsed_ms, logbuf.data)));
				pfree(logbuf.data);
				MemSet(&logbuf, 0, sizeof(logbuf));
			}
		}

		/* An error other than EINTR is not acceptable */
		if (n < 0)
		{
			if (select_errno == EINTR)
				continue;
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("interconnect error in select: %s",
							strerror(select_errno))));
		}

		/*
		 * check our connections that are accepted'd but no register message.
		 * we don't know which motion node these apply to until we actually
		 * receive the REGISTER message.  this is why they are all in a single
		 * list.
		 *
		 * We used to bail out of the while loop when incoming_count hit
		 * expectedTotalIncoming, but that causes problems if some connections
		 * are left over -- better to just process them here.
		 */
		cell = list_head(interconnect_context->incompleteConns);
		foreach(cell, interconnect_context->incompleteConns)
		{
			if (n <= 0)
			{
				break;
			}

			conn = (MotionConn *) lfirst(cell);
			tcp_conn = CONTAINER_OF(conn, MotionConnTCP, mConn);

			if (MPP_FD_ISSET(conn->sockfd, &rset))
			{
				n--;
				if (readRegisterMessage(interconnect_context, conn))
				{
					/*
					 * We're done with this connection (either it is bogus
					 * (and has been dropped), or we've added it to the
					 * appropriate hash table)
					 */
					interconnect_context->incompleteConns = foreach_delete_current(interconnect_context->incompleteConns, cell);

					/* is the connection ready ? */
					if (conn->sockfd != -1)
						incoming_count++;

					if (conn->pBuff)
						pfree(conn->pBuff);
					/* Free temporary MotionConn storage. */
					pfree(tcp_conn);
				}
			}
		}

		/*
		 * Someone tickling our listener port?  Accept pending connections.
		 */
		if (MPP_FD_ISSET(TCP_listenerFd, &rset))
		{
			n--;
			while ((conn = acceptIncomingConnection()) != NULL)
			{
				/*
				 * get the connection read for a subsequent call to
				 * ReadRegisterMessage()
				 */
				conn->state = mcsRecvRegMsg;
				conn->msgSize = sizeof(RegisterMessage);
				conn->msgPos = conn->pBuff;
				conn->remapper = CreateTupleRemapper();

				interconnect_context->incompleteConns = lappend(interconnect_context->incompleteConns, conn);
			}
		}

		/*
		 * Check our outgoing connections.
		 */
		i = 0;
		while (n > 0 &&
			   outgoing_count < expectedTotalOutgoing &&
			   i < sendingChunkTransportState->numConns)
		{						/* loop to check outgoing connections */
			getMotionConn(sendingChunkTransportState, i, &conn);
			i++;
			tcp_conn = CONTAINER_OF(conn, MotionConnTCP, mConn);
			switch (conn->state)
			{
				case mcsConnecting:
					/* Has connect() succeeded or failed? */
					if (MPP_FD_ISSET(conn->sockfd, &wset) ||
						MPP_FD_ISSET(conn->sockfd, &eset))
					{
						n--;
						updateOutgoingConnection(interconnect_context, sendingChunkTransportState, conn, -1);
						switch (conn->state)
						{
							case mcsSetupOutgoingConnection:
								/* Failed.  Wait awhile before retrying. */
								tcp_conn->wakeup_ms = (iteration - 1) * 1000 + elapsed_ms;
								break;
							case mcsSendRegMsg:
								/* Connected, but reg msg not fully sent. */
								tcp_conn->wakeup_ms = 0;
								break;
							case mcsStarted:
								/* Connected, sent reg msg, ready to rock. */
								outgoing_count++;
								break;
							default:
								elog(FATAL, "SetupTCPInterconnect: bad outgoing state");
						}
					}
					break;

				case mcsSendRegMsg:
					/* Ready to continue sending? */
					if (MPP_FD_ISSET(conn->sockfd, &wset))
					{
						n--;
						sendRegisterMessage(interconnect_context, sendingChunkTransportState, conn);
						if (conn->state == mcsStarted)
							outgoing_count++;
					}
					break;

				default:
					break;
			}

		}						/* loop to check outgoing connections */

		/* By now we have dealt with all the events reported by select(). */
		if (n != 0)
			elog(FATAL, "SetupTCPInterconnect: extra select events.");
	}							/* select() loop */

	/*
	 * if everything really got setup properly then we shouldn't have any
	 * incomplete connections.
	 *
	 * XXX: In some cases (when the previous query got 'fast-track cancelled'
	 * because of an error during setup) we can wind up with connections here
	 * which ought to have been cleaned up. These connections should be closed
	 * out here. It would obviously be better if we could avoid these
	 * connections in the first place!
	 */
	if (list_length(interconnect_context->incompleteConns) != 0)
	{
		if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
			elog(DEBUG2, "Incomplete connections after known connections done, cleaning %d",
				 list_length(interconnect_context->incompleteConns));

		foreach(cell, interconnect_context->incompleteConns)
		{
			conn = (MotionConn *) lfirst(cell);
			tcp_conn = CONTAINER_OF(conn, MotionConnTCP, mConn);

			if (conn->sockfd != -1)
			{
				flushIncomingData(conn->sockfd);
				shutdown(conn->sockfd, SHUT_WR);
				closesocket(conn->sockfd);
				conn->sockfd = -1;
			}

			if (conn->pBuff)
				pfree(conn->pBuff);
			pfree(tcp_conn);

			interconnect_context->incompleteConns = foreach_delete_current(interconnect_context->incompleteConns, cell);
		}
	}

	interconnect_context->activated = true;

	if (gp_log_interconnect >= GPVARS_VERBOSITY_TERSE)
	{
		elapsed_ms = gp_get_elapsed_ms(&startTime);
		if (gp_log_interconnect >= GPVARS_VERBOSITY_VERBOSE ||
			elapsed_ms >= 0.1 * 1000 * interconnect_setup_timeout)
			elog(LOG, "SetupTCPInterconnect+" UINT64_FORMAT "ms: Activated %d incoming, "
				 "%d outgoing routes.",
				 elapsed_ms, incoming_count, outgoing_count);
	}

	estate->interconnect_context = interconnect_context;
	estate->es_interconnect_is_setup = true;
}								/* SetupTCPInterconnect */

void
SetupInterconnectTCP(EState *estate)
{
	interconnect_handle_t *h;
	MemoryContext oldContext;

	if (estate->interconnect_context)
	{
		elog(ERROR, "SetupInterconnectTCP: already initialized.");
	}
	else if (!estate->es_sliceTable)
	{
		elog(ERROR, "SetupInterconnectTCP: no slice table ?");
	}

	h = allocate_interconnect_handle(TeardownInterconnectTCP);

	Assert(InterconnectContext != NULL);
	oldContext = MemoryContextSwitchTo(InterconnectContext);

	SetupTCPInterconnect(estate);

	MemoryContextSwitchTo(oldContext);
	h->interconnect_context = estate->interconnect_context;
}

/* TeardownInterconnect() function is used to cleanup interconnect resources that
 * were allocated during SetupInterconnectTCP().  This function should ALWAYS be
 * called after SetupInterconnectTCP to avoid leaking resources (like sockets)
 * even if SetupInterconnectTCP did not complete correctly.  As a result, this
 * function must complete successfully even if SetupInterconnectTCP didn't.
 *
 * SetupInterconnectTCP() always gets called under the ExecutorState MemoryContext.
 * This context is destroyed at the end of the query and all memory that gets
 * allocated under it is free'd.  We don't have have to worry about pfree() but
 * we definitely have to worry about socket resources.
 */
void
TeardownTCPInterconnect(ChunkTransportState * transportStates, bool hasErrors)
{
	ListCell   *cell;
	ChunkTransportStateEntry *pEntry = NULL;
	int			i;
	ExecSlice  *mySlice;
	MotionConn *conn = NULL;

	if (transportStates == NULL || transportStates->sliceTable == NULL)
	{
		elog(LOG, "TeardownTCPInterconnect: missing slice table.");
		return;
	}

	/*
	 * if we're already trying to clean up after an error -- don't allow
	 * signals to interrupt us
	 */
	if (hasErrors)
		HOLD_INTERRUPTS();

	mySlice = &transportStates->sliceTable->slices[transportStates->sliceId];

	/* Log the start of TeardownTCPInterconnect. */
	if (gp_log_interconnect >= GPVARS_VERBOSITY_TERSE)
	{
		int			elevel = 0;

		if (hasErrors || !transportStates->activated)
		{
			if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
				elevel = LOG;
			else
				elevel = DEBUG1;
		}
		else if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
			elevel = DEBUG4;

		if (elevel)
			ereport(elevel, (errmsg("Interconnect seg%d slice%d cleanup state: "
									"%s; setup was %s",
									GpIdentity.segindex, mySlice->sliceIndex,
									hasErrors ? "error" : "normal",
									transportStates->activated ? "completed" : "exited")));

		/* if setup did not complete, log the slicetable */
		if (!transportStates->activated &&
			gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
			elog_node_display(DEBUG3, "local slice table", transportStates->sliceTable, true);
	}

	/*
	 * phase 1 mark all sockets (senders and receivers) with shutdown(2),
	 * start with incomplete connections (if any).
	 */

	/*
	 * The incompleteConns list is only used as a staging area for MotionConns
	 * during by SetupInterconnect().  So we only expect to have entries here
	 * if SetupInterconnect() did not finish correctly.
	 *
	 * NOTE: we don't use foreach() here because we want to trim from the list
	 * as we go.
	 */
	if (transportStates->incompleteConns &&
		gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
		elog(DEBUG2, "Found incomplete conn. length %d", list_length(transportStates->incompleteConns));

	/*
	 * These are connected inbound peers that we haven't dealt with quite yet
	 */
	while ((cell = list_head(transportStates->incompleteConns)) != NULL)
	{
		MotionConn *conn = (MotionConn *) lfirst(cell);

		/* they're incomplete, so just slam them shut. */
		if (conn->sockfd != -1)
		{
			flushIncomingData(conn->sockfd);
			shutdown(conn->sockfd, SHUT_WR);
			closesocket(conn->sockfd);
			conn->sockfd = -1;
		}

		/* free up the tuple remapper */
		if (conn->remapper)
		{
			DestroyTupleRemapper(conn->remapper);
			conn->remapper = NULL;
		}

		/*
		 * The list operations are kind of confusing (see list.c), we could
		 * alternatively write the following line as:
		 *
		 * incompleteConns = list_delete_cell(incompleteConns, cell, NULL); or
		 * incompleteConns = list_delete_first(incompleteConns); or
		 * incompleteConns = list_delete_ptr(incompleteConns, conn)
		 */
		transportStates->incompleteConns = list_delete(transportStates->incompleteConns, conn);
	}

	list_free(transportStates->incompleteConns);
	transportStates->incompleteConns = NIL;

	/*
	 * Now "normal" connections which made it through our peer-registration
	 * step. With these we have to worry about "in-flight" data.
	 */
	if (mySlice->parentIndex != -1)
	{
		/* cleanup a Sending motion node. */
		if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
			elog(DEBUG3, "Interconnect seg%d slice%d closing connections to slice%d",
				 GpIdentity.segindex, mySlice->sliceIndex, mySlice->parentIndex);

		getChunkTransportState(transportStates, mySlice->sliceIndex, &pEntry);

		for (i = 0; i < pEntry->numConns; i++)
		{
			getMotionConn(pEntry, i, &conn);
			if (conn->sockfd >= 0)
				shutdown(conn->sockfd, SHUT_WR);

			/* free up the tuple remapper */
			if (conn->remapper)
			{
				DestroyTupleRemapper(conn->remapper);
				conn->remapper = NULL;
			}
		}
	}

	/*
	 * cleanup all of our Receiving Motion nodes, these get closed immediately
	 * (the receiver know for real if they want to shut down -- they aren't
	 * going to be processing any more data).
	 */
	foreach(cell, mySlice->children)
	{
		ExecSlice  *aSlice;
		int			childId = lfirst_int(cell);

		aSlice = &transportStates->sliceTable->slices[childId];

		getChunkTransportState(transportStates, aSlice->sliceIndex, &pEntry);

		if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
			elog(DEBUG3, "Interconnect closing connections from slice%d",
				 aSlice->sliceIndex);

		/*
		 * receivers know that they no longer care about data from below ...
		 * so we can safely discard data queued in both directions
		 */
		for (i = 0; i < pEntry->numConns; i++)
		{
			getMotionConn(pEntry, i, &conn);

			if (conn->sockfd >= 0)
			{
				flushIncomingData(conn->sockfd);
				shutdown(conn->sockfd, SHUT_WR);

				closesocket(conn->sockfd);
				conn->sockfd = -1;

				/* free up the tuple remapper */
				if (conn->remapper)
				{
					DestroyTupleRemapper(conn->remapper);
					conn->remapper = NULL;
				}

			}
		}
		removeChunkTransportState(transportStates, aSlice->sliceIndex);
		pfree(pEntry->conns);
	}

	/*
	 * phase 2: wait on all sockets for completion, when complete call close
	 * and free (if required)
	 */
	if (mySlice->parentIndex != -1)
	{
		/* cleanup a Sending motion node. */
		getChunkTransportState(transportStates, mySlice->sliceIndex, &pEntry);

		/*
		 * On a normal teardown routine, sender has sent an EOS packet and
		 * disabled further send operations on phase 1. sender can't close the
		 * connection immediately because EOS packet or data packets within
		 * the kernel sending buffer may be lost on some platform if sender
		 * close the connection totally.
		 *
		 * The correct way is sender blocks on the connection until receivers
		 * get the EOS packets and close the peer, then it's safe for sender
		 * to close the connection totally.
		 *
		 * If some errors are happening, senders can skip this step to avoid
		 * hung issues, QD will take care of the error handling.
		 */
		if (!hasErrors)
			waitOnOutbound(pEntry);

		for (i = 0; i < pEntry->numConns; i++)
		{
			getMotionConn(pEntry, i, &conn);

			if (conn->sockfd >= 0)
			{
				closesocket(conn->sockfd);
				conn->sockfd = -1;
			}
		}
		pEntry = removeChunkTransportState(transportStates, mySlice->sliceIndex);
	}

	/*
	 * If there are clients waiting on our listener; we *must* disconnect
	 * them; otherwise we'll be out of sync with the client (we may accept
	 * them on a subsequent query!)
	 */
	if (TCP_listenerFd != -1)
		flushInterconnectListenerBacklog();

	transportStates->activated = false;
	transportStates->sliceTable = NULL;

#ifdef ENABLE_IC_PROXY
	ic_proxy_backend_close_context(transportStates);
#endif							/* ENABLE_IC_PROXY */

	if (transportStates->states != NULL)
		pfree(transportStates->states);
	if (transportStates->conn_sent_record_typmod)
		hash_destroy(transportStates->conn_sent_record_typmod);

	pfree(transportStates);

	if (hasErrors)
		RESUME_INTERRUPTS();

#ifdef AMS_VERBOSE_LOGGING
	if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
		elog(DEBUG4, "TeardownTCPInterconnect successful");
#endif
}

void
TeardownInterconnectTCP(ChunkTransportState * transportStates,
						bool hasErrors)
{
	/* TODO: should pass interconnect_handle_t as arg? */
	interconnect_handle_t *h = find_interconnect_handle(transportStates);

	TeardownTCPInterconnect(transportStates, hasErrors);

	if (h != NULL) {
		destroy_interconnect_handle(h);
	}
}

#ifdef AMS_VERBOSE_LOGGING
void
dumpEntryConnections(int elevel, ChunkTransportStateEntry * pEntry)
{
	int			i;
	MotionConn *mConn = NULL;
	MotionConnTCP *conn = NULL;

	for (i = 0; i < pEntry->numConns; i++)
	{
		getMotionConn(pEntry, i, &mConn);
		conn = CONTAINER_OF(mConn, MotionConnTCP, mConn);
		if (conn->mConn.sockfd == -1 &&
			conn->mConn.state == mcsNull)
			elog(elevel, "... motNodeId=%d conns[%d]:         not connected",
				 pEntry->motNodeId, i);
		else
			elog(elevel, "... motNodeId=%d conns[%d]:  "
				 "%d pid=%d sockfd=%d remote=%s local=%s",
				 pEntry->motNodeId, i,
				 conn->mConn.remoteContentId,
				 conn->mConn.cdbProc ? conn->mConn.cdbProc->pid : 0,
				 conn->mConn.sockfd,
				 conn->mConn.remoteHostAndPort,
				 conn->localHostAndPort);
	}
}

static void
print_connection(ChunkTransportState * transportStates, int fd, const char *msg)
{
	struct sockaddr_in local,
				remote;

	socklen_t	len;
	int			errlevel = transportStates->teardownActive ? LOG : ERROR;

	len = sizeof(remote);
	if (getpeername(fd, (struct sockaddr *) &remote, &len) < 0)
	{
		elog(errlevel, "print_connection(%d, %s): can't get peername err: %m",
			 fd, msg);
	}

	len = sizeof(local);
	if (getsockname(fd, (struct sockaddr *) &local, &len) < 0)
	{
		elog(errlevel, "print_connection(%d, %s): can't get localname err: %m",
			 fd, msg);
	}

	elog(DEBUG2, "%s: w/ports (%d/%d)",
		 msg, ntohs(local.sin_port), ntohs(remote.sin_port));
}
#endif

static void
format_fd_set(StringInfo buf, int nfds, mpp_fd_set * fds, char *pfx, char *sfx)
{
	int			i;
	bool		first = true;

	appendStringInfoString(buf, pfx);
	for (i = 1; i < nfds; i++)
	{
		if (MPP_FD_ISSET(i, fds))
		{
			if (!first)
				appendStringInfoChar(buf, ',');
			appendStringInfo(buf, "%d", i);
			first = false;
		}
	}

	appendStringInfoString(buf, sfx);
}

static void
flushInterconnectListenerBacklog(void)
{
	int			pendingConn,
				newfd,
				i;
	mpp_fd_set	rset;
	struct timeval timeout;

	do
	{
		MPP_FD_ZERO(&rset);
		MPP_FD_SET(TCP_listenerFd, &rset);
		timeout.tv_sec = 0;
		timeout.tv_usec = 0;

		pendingConn = select(TCP_listenerFd + 1, (fd_set *) &rset, NULL, NULL, &timeout);
		if (pendingConn > 0)
		{
			for (i = 0; i < pendingConn; i++)
			{
				struct sockaddr_storage remoteAddr;
				struct sockaddr_storage localAddr;
				char		remoteHostAndPort[64];
				char		localHostAndPort[64];
				socklen_t	addrsize;

				addrsize = sizeof(remoteAddr);
				newfd = accept(TCP_listenerFd, (struct sockaddr *) &remoteAddr, &addrsize);
				if (newfd < 0)
				{
					ereport(DEBUG3, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
									 errmsg("Interconnect error while clearing incoming connections."),
									 errdetail("%s sockfd=%d: %m", "accept", newfd)));
					continue;
				}

				if (gp_log_interconnect >= GPVARS_VERBOSITY_VERBOSE)
				{
					/* Get remote and local host:port strings for message. */
					format_sockaddr(&remoteAddr, remoteHostAndPort,
									sizeof(remoteHostAndPort));
					addrsize = sizeof(localAddr);
					if (getsockname(newfd, (struct sockaddr *) &localAddr, &addrsize))
					{
						ereport(LOG,
								(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
								 errmsg("interconnect error while clearing incoming connections"),
								 errdetail("getsockname sockfd=%d remote=%s: %m",
										   newfd, remoteHostAndPort)));
					}
					else
					{
						format_sockaddr(&localAddr, localHostAndPort,
										sizeof(localHostAndPort));
						ereport(DEBUG2, (errmsg("Interconnect clearing incoming connection "
												"from remote=%s to local=%s.  sockfd=%d.",
												remoteHostAndPort, localHostAndPort,
												newfd)));
					}
				}

				/* make socket non-blocking */
				if (!pg_set_noblock(newfd))
				{
					elog(LOG, "During incoming queue flush, could not set non-blocking.");
				}
				else
				{
					/* shutdown this socket */
					flushIncomingData(newfd);
				}

				shutdown(newfd, SHUT_WR);
				closesocket(newfd);
			}
		}
		else if (pendingConn < 0 && errno != EINTR)
		{
			ereport(LOG,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("interconnect error during listener cleanup"),
					 errdetail("select sockfd=%d: %m", TCP_listenerFd)));
		}

		/*
		 * now we either loop through for another check (on EINTR or if we
		 * cleaned one client) or we're done
		 */
	}
	while (pendingConn != 0);
}

/*
 * Wait for our peer to close the socket (at which point our select(2)
 * will tell us that the socket is ready to read, and the socket recv
 * will return 0 or a 'stop' message.
 *
 * This works without the select, but burns tons of CPU doing nothing
 * useful.
 *
 * ----
 * The way it used to work, is we used CHECK_FOR_INTERRUPTS(), and
 * wrapped it in PG_TRY: We *must* return locally; otherwise
 * TeardownInterconnect() can't exit cleanly. So we wrap our
 * cancel-detection checks for interrupts with a PG_TRY block.
 *
 * By swallowing the non-local return on cancel, we lose the "cancel"
 * state (CHECK_FOR_INTERRUPTS() clears QueryCancelPending()). So we
 * should just check QueryCancelPending here ... and avoid calling
 * CHECK_FOR_INTERRUPTS().
 *
 * ----
 *
 * Now we just check explicitly for interrupts (which is, as far as I
 * can tell, the only interrupt-driven state change we care
 * about). This should give us notification of ProcDiePending and
 * QueryCancelPending
 *
 * XXX: Consider using something like WaitLatchOrSocket() instead of select().
 */
static void
waitOnOutbound(ChunkTransportStateEntry * pEntry)
{
	MotionConn *conn = NULL;

	mpp_fd_set	waitset,
				curset;
	int			maxfd = -1;
	int			i,
				n,
				conn_count = 0;
	struct timeval endtime;

	SIMPLE_FAULT_INJECTOR("waitOnOutbound");

	MPP_FD_ZERO(&waitset);

	for (i = 0; i < pEntry->numConns; i++)
	{
		getMotionConn(pEntry, i, &conn);

		if (conn->sockfd >= 0)
		{
			MPP_FD_SET(conn->sockfd, &waitset);
			if (conn->sockfd > maxfd)
				maxfd = conn->sockfd;
			conn_count++;
		}
	}

	gettimeofday(&endtime, NULL);
	endtime.tv_sec += Gp_interconnect_transmit_timeout;

	for (;;)
	{
		int			saved_err;
		struct timeval timeout;
		struct timeval now;
		int64		timeoutval;

		if (conn_count == 0)
			return;

		if (CancelRequested() || QueryFinishPending)
		{
#ifdef AMS_VERBOSE_LOGGING
			elog(DEBUG3, "waitOnOutbound(): interrupt pending fast-track");
#endif
			return;
		}

		gettimeofday(&now, NULL);
		timeoutval = (endtime.tv_sec * 1000000 + endtime.tv_usec) - (now.tv_sec * 1000000 + now.tv_usec);
		if (timeoutval <= 0)
		{
			/*
			 * We timed out trying to receive a final response from the motion
			 * receiver (either a 'stop' message or 0). Since this response
			 * confirms that the receiver has received the EOS from this sender
			 * (and all preceding data), receipt of this response is vital.
			 * Hence, we error out.
			 */
			ereport(ERROR,
					errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					errmsg("timed out waiting for response from motion receiver during TCP interconnect teardown"),
					errdetail("%d connection(s) with pending response after %d seconds",
							  conn_count,
							  Gp_interconnect_transmit_timeout));
		}
		timeout.tv_sec = timeoutval / 1000000;
		timeout.tv_usec = timeoutval % 1000000;

		memcpy(&curset, &waitset, sizeof(mpp_fd_set));

		n = select(maxfd + 1, (fd_set *) &curset, NULL, NULL, &timeout);
		if (n < 0)
		{
			if (errno == EINTR)
				continue;

			saved_err = errno;

			if (CancelRequested() || QueryFinishPending)
				return;

			/* Something unexpected, but probably not horrible warn and return */
			elog(LOG, "TeardownTCPInterconnect: waitOnOutbound select errno=%d", saved_err);
			break;
		}
		if (n == 0)
		{
			/*
			 * We timed out trying to receive a final response from the motion
			 * receiver (either a 'stop' message or 0). Since this response
			 * confirms that the receiver has received the EOS from this sender
			 * (and all preceding data), receipt of this response is vital.
			 * Hence, we error out.
			 */
			ereport(ERROR,
					errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					errmsg("timed out waiting for response from motion receiver during TCP interconnect teardown"),
					errdetail("%d connection(s) with pending response after %d seconds",
							  conn_count,
							  Gp_interconnect_transmit_timeout));
		}

		for (i = 0; i < pEntry->numConns; i++)
		{
			getMotionConn(pEntry, i, &conn);

			if (conn->sockfd >= 0 && MPP_FD_ISSET(conn->sockfd, &curset))
			{
				int			count;
				char		buf;

				/* ready to read. */
				count = recv(conn->sockfd, &buf, sizeof(buf), 0);

				if (count == 0 || count == 1)	/* done ! */
				{
					/* got a stop message */
					AssertImply(count == 1, buf == 'S');

					MPP_FD_CLR(conn->sockfd, &waitset);
					/* we may have finished */
					conn_count--;
					continue;
				}
				else if (count < 0 && (errno == EAGAIN || errno == EINTR))
					continue;

				/*
				 * Something unexpected, but probably not horrible warn and
				 * return
				 */
				MPP_FD_CLR(conn->sockfd, &waitset);
				/* we may have finished */
				conn_count--;
				elog(LOG, "TeardownTCPInterconnect: waitOnOutbound %s: %m", "recv");
				continue;
			}
		}
	}

	return;
}

void
DeregisterReadInterestTCP(ChunkTransportState * transportStates,
						  int motNodeID,
						  int srcRoute,
						  const char *reason)
{
	ChunkTransportStateEntry *pEntry = NULL;
	MotionConn *conn = NULL;

	if (!transportStates)
	{
		elog(FATAL, "DeregisterReadInterest: no transport states");
	}

	if (!transportStates->activated)
		return;

	getChunkTransportState(transportStates, motNodeID, &pEntry);
	getMotionConn(pEntry, srcRoute, &conn);

	if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
	{
		elog(DEBUG3, "Interconnect finished receiving "
			 "from seg%d slice%d %s pid=%d sockfd=%d; %s",
			 conn->remoteContentId,
			 pEntry->sendSlice->sliceIndex,
			 conn->remoteHostAndPort,
			 conn->cdbProc->pid,
			 conn->sockfd,
			 reason);
	}

	/*
	 * we also mark the connection as "done." The way synchronization works is
	 * strange. On QDs the "teardown" doesn't get called until all segments
	 * are finished, which means that we need some way for the QEs to know
	 * that Teardown should complete, otherwise we deadlock the entire query
	 * (QEs wait in their Teardown calls, while the QD waits for them to
	 * finish)
	 */
	shutdown(conn->sockfd, SHUT_WR);

	MPP_FD_CLR(conn->sockfd, &pEntry->readSet);
	return;
}

void
SendStopMessageTCP(ChunkTransportState * transportStates, int16 motNodeID)
{
	ChunkTransportStateEntry *pEntry = NULL;
	MotionConn *conn = NULL;
	int			i;
	char		m = 'S';
	ssize_t		written;

	SIMPLE_FAULT_INJECTOR("doSendStopMessageTCP");

	getChunkTransportState(transportStates, motNodeID, &pEntry);
	Assert(pEntry);

	if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
		elog(DEBUG3, "Interconnect needs no more input from slice%d; notifying senders to stop.",
			 motNodeID);

	/*
	 * Note: we're only concerned with receivers here.
	 */
	for (i = 0; i < pEntry->numConns; i++)
	{
		getMotionConn(pEntry, i, &conn);

		if (conn->sockfd >= 0 &&
			MPP_FD_ISSET(conn->sockfd, &pEntry->readSet))
		{
			/* someone is trying to send stuff to us, let's stop 'em */
			while ((written = send(conn->sockfd, &m, sizeof(m), 0)) < 0)
			{
				if (errno == EINTR)
				{
					ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);
					continue;
				}
				else
					break;
			}

			if (written != sizeof(m))
			{
				/*
				 * how can this happen ? the kernel buffer should be empty in
				 * the send direction
				 */
				elog(LOG, "SendStopMessage: failed on write.  %m");
			}
		}
		/* CRITICAL TO AVOID DEADLOCK */
		DeregisterReadInterestTCP(transportStates, motNodeID, i,
								  "no more input needed");
	}
}

TupleChunkListItem
RecvTupleChunkTCP(MotionConn * conn, ChunkTransportState * transportStates)
{
	TupleChunkListItem tcItem;
	TupleChunkListItem firstTcItem = NULL;
	TupleChunkListItem lastTcItem = NULL;
	uint32		tcSize;
	int			bytesProcessed = 0;


	/* read the packet in from the network. */
	readPacket(conn, transportStates);

	/* go through and form us some TupleChunks. */
	bytesProcessed = PACKET_HEADER_SIZE;

#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG5, "recvtuple chunk recv bytes %d msgsize %d conn->pBuff %p conn->msgPos: %p",
		 conn->recvBytes, conn->msgSize, conn->pBuff, conn->msgPos);
#endif

	while (bytesProcessed != conn->msgSize)
	{
		if (conn->msgSize - bytesProcessed < TUPLE_CHUNK_HEADER_SIZE)
		{
			elog(LOG, "Interconnect parse details(TCP/PROXY): pkt->len %d",
				 *((int32 *) conn->pBuff));

			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("interconnect error parsing message: insufficient data received"),
					 errdetail("conn->msgSize %d bytesProcessed %d < chunk-header %d",
							   conn->msgSize, bytesProcessed, TUPLE_CHUNK_HEADER_SIZE)));
		}

		tcSize = TUPLE_CHUNK_HEADER_SIZE + (*(uint16 *) (conn->msgPos + bytesProcessed));

		/* sanity check */
		if (tcSize > Gp_max_packet_size)
		{
			/*
			 * see MPP-720: it is possible that our message got messed up by a
			 * cancellation ?
			 */
			ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);

			/*
			 * MPP-4010: add some extra debugging.
			 */
			if (lastTcItem != NULL)
				elog(LOG, "Interconnect error parsing message: last item length %d inplace %p", lastTcItem->chunk_length, lastTcItem->inplace);
			else
				elog(LOG, "Interconnect error parsing message: no last item");

			elog(LOG, "Interconnect parse details(TCP/PROXY): pkt->len %d",
				 *((int32 *) conn->pBuff));

			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("interconnect error parsing message"),
					 errdetail("tcSize %d > max %d header %d processed %d/%d from %p",
							   tcSize, Gp_max_packet_size,
							   TUPLE_CHUNK_HEADER_SIZE, bytesProcessed,
							   conn->msgSize, conn->msgPos)));
		}


		/*
		 * we only check for interrupts here when we don't have a guaranteed
		 * full-message
		 */

		if (tcSize >= conn->msgSize)
		{
			/*
			 * see MPP-720: it is possible that our message got messed up by a
			 * cancellation ?
			 */
			ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);

			elog(LOG, "Interconnect parse details(TCP/PROXY): pkt->len %d",
				 *((int32 *) conn->pBuff));

			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("interconnect error parsing message"),
					 errdetail("tcSize %d >= conn->msgSize %d",
							   tcSize, conn->msgSize)));
		}
		Assert(tcSize < conn->msgSize);

		/*
		 * We store the data inplace, and handle any necessary copying later
		 * on
		 */
		tcItem = (TupleChunkListItem) palloc(sizeof(TupleChunkListItemData));

		tcItem->p_next = NULL;
		tcItem->chunk_length = tcSize;
		tcItem->inplace = (char *) (conn->msgPos + bytesProcessed);

		bytesProcessed += tcSize;

		if (firstTcItem == NULL)
		{
			firstTcItem = tcItem;
			lastTcItem = tcItem;
		}
		else
		{
			lastTcItem->p_next = tcItem;
			lastTcItem = tcItem;
		}
	}

	conn->recvBytes -= conn->msgSize;
	if (conn->recvBytes != 0)
	{
#ifdef AMS_VERBOSE_LOGGING
		elog(DEBUG5, "residual message %d bytes", conn->recvBytes);
#endif
		conn->msgPos += conn->msgSize;
	}

	conn->msgSize = 0;

	return firstTcItem;
}

TupleChunkListItem
RecvTupleChunkFromTCP(ChunkTransportState * transportStates,
					  int16 motNodeID,
					  int16 srcRoute)
{
	ChunkTransportStateEntry *pEntry = NULL;
	MotionConn *conn = NULL;

	/* check em' */
	ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);

#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG5, "RecvTupleChunkFromTCP(motNodID=%d, srcRoute=%d)", motNodeID, srcRoute);
#endif

	getChunkTransportState(transportStates, motNodeID, &pEntry);
	getMotionConn(pEntry, srcRoute, &conn);

	return RecvTupleChunkTCP(conn, transportStates);
}

TupleChunkListItem
RecvTupleChunkFromAnyTCP(ChunkTransportState * transportStates,
						 int16 motNodeID,
						 int16 *srcRoute)
{
	ChunkTransportStateEntry *pChunkEntry = NULL;
	ChunkTransportStateEntryTCP *pEntry = NULL;
	TupleChunkListItem tcItem;
	MotionConn *conn = NULL;
	mpp_fd_set	rset;
	int			n,
				i,
				index;
	bool		skipSelect = false;
	int			nwaitfds = 0;
	int		   *waitFds = NULL;

#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG5, "RecvTupleChunkFromAnyTCP(motNodeId=%d)", motNodeID);
#endif

	getChunkTransportState(transportStates, motNodeID, &pChunkEntry);
	pEntry = CONTAINER_OF(pChunkEntry, ChunkTransportStateEntryTCP, entry);
	Assert(pEntry);

	int			retry = 0;

	do
	{
		/* Every 2 seconds */
		if (Gp_role == GP_ROLE_DISPATCH && retry++ > 4)
		{
			retry = 0;
			/* check to see if the dispatcher should cancel */
			checkForCancelFromQD(transportStates);
		}

		struct timeval timeout = tval;
		int			nfds = pEntry->highReadSock;

		/* make sure we check for these. */
		ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);

		memcpy(&rset, &pEntry->entry.readSet, sizeof(mpp_fd_set));

		/*
		 * since we may have data in a local buffer, we may be able to
		 * short-circuit the select() call (and if we don't do this we may
		 * wait when we have data ready, since it has already been read)
		 */
		for (i = 0; i < pEntry->entry.numConns; i++)
		{
			getMotionConn(&pEntry->entry, i, &conn);

			if (conn->sockfd >= 0 &&
				MPP_FD_ISSET(conn->sockfd, &rset) &&
				conn->recvBytes != 0)
			{
				/* we have data on this socket, let's short-circuit our select */
				MPP_FD_ZERO(&rset);
				MPP_FD_SET(conn->sockfd, &rset);

				skipSelect = true;
			}
		}
		if (skipSelect)
			break;

		/*
		 * Also monitor the events on dispatch fds, eg, errors or sequence
		 * request from QEs.
		 */
		nwaitfds = 0;
		if (Gp_role == GP_ROLE_DISPATCH)
		{
			waitFds = cdbdisp_getWaitSocketFds(transportStates->estate->dispatcherState, &nwaitfds);
			if (waitFds != NULL)
				for (i = 0; i < nwaitfds; i++)
				{
					MPP_FD_SET(waitFds[i], &rset);
					/* record the max fd number for select() later */
					if (waitFds[i] > nfds)
						nfds = waitFds[i];
				}

		}

		/*
		 * GPDB_12_MERGE_FIXME: should use WaitEventSetWait() instead of
		 * select()
		 */
		/* follow the routine in ic_udpifc.c */
		/**
		 * Notice that: ic-proxy won't get any tuple if ic_proxy_addrs is empty
		 * So double check you have already configurate the proxy addresses
		 *
		 * check README.ic-proxy.md for more infos
		 */
		n = select(nfds + 1, (fd_set *) &rset, NULL, NULL, &timeout);
		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("interconnect error receiving an incoming packet"),
					 errdetail("%s: %m", "select")));
		}
		else if (n > 0 && nwaitfds > 0)
		{
			bool		need_check = false;

			for (i = 0; i < nwaitfds; i++)
				if (MPP_FD_ISSET(waitFds[i], &rset))
				{
					need_check = true;
					n--;
				}

			/* handle events on dispatch connection */
			if (need_check)
				checkForCancelFromQD(transportStates);
		}

		if (waitFds)
			pfree(waitFds);

#ifdef AMS_VERBOSE_LOGGING
		elog(DEBUG5, "RecvTupleChunkFromAnyTCP() select() returned %d ready sockets", n);
#endif
	} while (n < 1);

	/*
	 * We scan the file descriptors starting from where we left off in the
	 * last call (don't continually poll the first when others may be ready!).
	 */
	index = pEntry->entry.scanStart;
	for (i = 0; i < pEntry->entry.numConns; i++, index++)
	{
		/*
		 * avoid division ? index = ((scanStart + i) %
		 * pEntry->entry.numConns);
		 */
		if (index >= pEntry->entry.numConns)
		{
			index = 0;
		}

		getMotionConn(&pEntry->entry, index, &conn);

#ifdef AMS_VERBOSE_LOGGING
		if (!conn->stillActive)
		{
			elog(LOG, "RecvTupleChunkFromAnyTCP: trying to read on inactive socket %d", conn->sockfd);
		}
#endif

		if (conn->sockfd >= 0 &&
			MPP_FD_ISSET(conn->sockfd, &rset))
		{
#ifdef AMS_VERBOSE_LOGGING
			elog(DEBUG5, "RecvTupleChunkFromAnyTCP() (fd %d) %d/%d", conn->sockfd, motNodeID, index);
#endif
			tcItem = RecvTupleChunkTCP(conn, transportStates);

			*srcRoute = index;

			/*
			 * advance start point (avoid doing division/modulus operation
			 * here)
			 */
			pEntry->entry.scanStart = index + 1;

			return tcItem;
		}
	}

	/* we should never ever get here... */
	elog(FATAL, "RecvTupleChunkFromAnyTCP: didn't receive, and didn't get cancelled");
	return NULL;				/* keep the compiler happy */
}

/* See ml_ipc.h */
void
SendEOSTCP(ChunkTransportState * transportStates,
		   int motNodeID,
		   TupleChunkListItem tcItem)
{
	ChunkTransportStateEntry *pEntry = NULL;
	MotionConn *conn = NULL;
	int			i;

	if (!transportStates)
	{
		elog(FATAL, "SendEOSTCP: missing interconnect context.");
	}
	else if (!transportStates->activated && !transportStates->teardownActive)
	{
		elog(FATAL, "SendEOSTCP: context and teardown inactive.");
	}

	/* check em' */
	ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);

	getChunkTransportState(transportStates, motNodeID, &pEntry);

	if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
		elog(DEBUG3, "Interconnect seg%d slice%d sending end-of-stream to slice%d",
			 GpIdentity.segindex, motNodeID, pEntry->recvSlice->sliceIndex);

	/*
	 * we want to add our tcItem onto each of the outgoing buffers -- this is
	 * guaranteed to leave things in a state where a flush is *required*.
	 */
	doBroadcast(transportStates, pEntry, tcItem, NULL);

	/* now flush all of the buffers. */
	for (i = 0; i < pEntry->numConns; i++)
	{
		getMotionConn(pEntry, i, &conn);

		if (conn->sockfd >= 0 && conn->state == mcsStarted)
			flushBuffer(transportStates, pEntry, conn, motNodeID);

#ifdef AMS_VERBOSE_LOGGING
		elog(DEBUG5, "SendEOSTCP() Leaving");
#endif
	}

	return;
}

static bool
flushBuffer(ChunkTransportState * transportStates,
			ChunkTransportStateEntry * pEntry, MotionConn * conn, int16 motionId)
{
	char	   *sendptr;
	int			n,
				sent = 0;
	mpp_fd_set	wset;
	mpp_fd_set	rset;

#ifdef AMS_VERBOSE_LOGGING
	{
		struct timeval snapTime;

		gettimeofday(&snapTime, NULL);
		elog(DEBUG5, "----sending chunk @%s.%d time is %d.%d",
			 __FILE__, __LINE__, (int) snapTime.tv_sec, (int) snapTime.tv_usec);
	}
#endif

	/* first set header length */
	*(uint32 *) conn->pBuff = conn->msgSize;

	/* now send message */
	sendptr = (char *) conn->pBuff;
	sent = 0;
	do
	{
		struct timeval timeout;

		/* check for stop message or peer teardown before sending anything  */
		timeout.tv_sec = 0;
		timeout.tv_usec = 0;
		MPP_FD_ZERO(&rset);
		MPP_FD_SET(conn->sockfd, &rset);

		/*
		 * since timeout = 0, select returns imediately and no time is wasted
		 * waiting trying to send data on the network
		 */
		n = select(conn->sockfd + 1, (fd_set *) &rset, NULL, NULL, &timeout);
		/* handle errors at the write call, below */
		if (n > 0 && MPP_FD_ISSET(conn->sockfd, &rset))
		{
#ifdef AMS_VERBOSE_LOGGING
			print_connection(transportStates, conn->sockfd, "stop from");
#endif
			/* got a stop message */
			conn->stillActive = false;
			return false;
		}

		if ((n = send(conn->sockfd, sendptr + sent, conn->msgSize - sent, 0)) < 0)
		{
			int			send_errno = errno;

			ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);
			if (send_errno == EINTR)
				continue;
			if (send_errno == EWOULDBLOCK)
			{
				do
				{
					timeout = tval;

					ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);

					MPP_FD_ZERO(&rset);
					MPP_FD_ZERO(&wset);
					MPP_FD_SET(conn->sockfd, &wset);
					MPP_FD_SET(conn->sockfd, &rset);
					n = select(conn->sockfd + 1, (fd_set *) &rset, (fd_set *) &wset, NULL, &timeout);
					if (n < 0)
					{
						int			select_errno = errno;

						if (select_errno == EINTR)
							continue;

						/*
						 * if we got an error in teardown, ignore it: treat it
						 * as a stop message
						 */
						if (transportStates->teardownActive)
						{
#ifdef AMS_VERBOSE_LOGGING
							print_connection(transportStates, conn->sockfd, "stop from");
#endif
							conn->stillActive = false;
							return false;
						}
						ereport(ERROR,
								(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
								 errmsg("interconnect error writing an outgoing packet: %m"),
								 errdetail("Error during select() call (error: %d), for remote connection: contentId=%d at %s",
										   select_errno, conn->remoteContentId,
										   conn->remoteHostAndPort)));
					}

					/*
					 * as a sender... if there is something to read... it must
					 * mean its a StopSendingMessage or receiver has teared
					 * down the interconnect, we don't even bother to read it.
					 */
					if (MPP_FD_ISSET(conn->sockfd, &rset) || transportStates->teardownActive)
					{
#ifdef AMS_VERBOSE_LOGGING
						print_connection(transportStates, conn->sockfd, "stop from");
#endif
						conn->stillActive = false;
						return false;
					}
				} while (n < 1);
			}
			else
			{
				/*
				 * if we got an error in teardown, ignore it: treat it as a
				 * stop message
				 */
				if (transportStates->teardownActive)
				{
#ifdef AMS_VERBOSE_LOGGING
					print_connection(transportStates, conn->sockfd, "stop from");
#endif
					conn->stillActive = false;
					return false;
				}

				/* check whether receiver has teared down the interconnect */
				timeout.tv_sec = 0;
				timeout.tv_usec = 0;
				MPP_FD_ZERO(&rset);
				MPP_FD_SET(conn->sockfd, &rset);

				n = select(conn->sockfd + 1, (fd_set *) &rset, NULL, NULL, &timeout);

				/*
				 * as a sender... if there is something to read... it must
				 * mean its a StopSendingMessage or receiver has teared down
				 * the interconnect, we don't even bother to read it.
				 */
				if (n > 0 && MPP_FD_ISSET(conn->sockfd, &rset))
				{
#ifdef AMS_VERBOSE_LOGGING
					print_connection(transportStates, conn->sockfd, "stop from");
#endif
					/* got a stop message */
					conn->stillActive = false;
					return false;
				}

				ereport(ERROR,
						(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
						 errmsg("interconnect error writing an outgoing packet"),
						 errdetail("Error during send() call (error:%d) for remote connection: contentId=%d at %s",
								   send_errno, conn->remoteContentId,
								   conn->remoteHostAndPort)));
			}
		}
		else
		{
			sent += n;
		}
	} while (sent < conn->msgSize);

	conn->tupleCount = 0;
	conn->msgSize = PACKET_HEADER_SIZE;

	return true;
}

/* The Function sendChunk() is used to send a tcItem to a single
 * destination. Tuples often are *very small* we aggregate in our
 * local buffer before sending into the kernel.
 *
 * PARAMETERS
 *	 conn - MotionConn that the tcItem is to be sent to.
 *	 tcItem - message to be sent.
 *	 motionId - Node Motion Id.
 */
bool
SendChunkTCP(ChunkTransportState * transportStates, ChunkTransportStateEntry * pEntry, MotionConn * conn, TupleChunkListItem tcItem, int16 motionId)
{
	int			length = tcItem->chunk_length;

	Assert(conn->msgSize > 0);

#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG5, "SendChunkTCP: msgSize %d this chunk length %d", conn->msgSize, tcItem->chunk_length);
#endif

	if (conn->msgSize + length > Gp_max_packet_size)
	{
		if (!flushBuffer(transportStates, pEntry, conn, motionId))
			return false;
	}

	memcpy(conn->pBuff + conn->msgSize, tcItem->chunk_data, tcItem->chunk_length);
	conn->msgSize += length;

	conn->tupleCount++;
	return true;
}



/*
 * gp_set_monotonic_begin_time: set the beginTime and endTime to the current
 * time.
 */
static void
gp_set_monotonic_begin_time(GpMonotonicTime * time)
{
	time->beginTime.tv_sec = 0;
	time->beginTime.tv_usec = 0;
	time->endTime.tv_sec = 0;
	time->endTime.tv_usec = 0;

	gp_get_monotonic_time(time);

	time->beginTime.tv_sec = time->endTime.tv_sec;
	time->beginTime.tv_usec = time->endTime.tv_usec;
}


/*
 * gp_get_monotonic_time
 *    This function returns the time in the monotonic order.
 *
 * The new time is stored in time->endTime, which has a larger value than
 * the original value. The original endTime is lost.
 *
 * This function is intended for computing elapsed time between two
 * calls. It is not for getting the system time.
 */
static void
gp_get_monotonic_time(GpMonotonicTime * time)
{
	struct timeval newTime;
	int			status;

#if HAVE_LIBRT
	/* Use clock_gettime to return monotonic time value. */
	struct timespec ts;

	status = clock_gettime(CLOCK_MONOTONIC, &ts);

	newTime.tv_sec = ts.tv_sec;
	newTime.tv_usec = ts.tv_nsec / 1000;

#else

	gettimeofday(&newTime, NULL);
	status = 0;					/* gettimeofday always succeeds. */

#endif

	if (status == 0 &&
		timeCmp(&time->endTime, &newTime) < 0)
	{
		time->endTime.tv_sec = newTime.tv_sec;
		time->endTime.tv_usec = newTime.tv_usec;
	}
	else
	{
		time->endTime.tv_usec = time->endTime.tv_usec + 1;

		time->endTime.tv_sec = time->endTime.tv_sec +
			(time->endTime.tv_usec / USECS_PER_SECOND);
		time->endTime.tv_usec = time->endTime.tv_usec % USECS_PER_SECOND;
	}
}

/*
 * Compare two times.
 *
 * If t1 > t2, return 1.
 * If t1 == t2, return 0.
 * If t1 < t2, return -1;
 */
static inline int
timeCmp(struct timeval *t1, struct timeval *t2)
{
	if (t1->tv_sec == t2->tv_sec &&
		t1->tv_usec == t2->tv_usec)
		return 0;

	if (t1->tv_sec > t2->tv_sec ||
		(t1->tv_sec == t2->tv_sec &&
		 t1->tv_usec > t2->tv_usec))
		return 1;

	return -1;
}

/*
 * gp_get_elapsed_us -- return the elapsed time in microseconds
 * after the given time->beginTime.
 *
 * If time->beginTime is not set (0), then return 0.
 *
 * Note that the beginTime is not changed, but the endTime is set
 * to the current time.
 */
static inline uint64
gp_get_elapsed_us(GpMonotonicTime * time)
{
	if (time->beginTime.tv_sec == 0 &&
		time->beginTime.tv_usec == 0)
		return 0;

	gp_get_monotonic_time(time);

	return ((time->endTime.tv_sec - time->beginTime.tv_sec) * USECS_PER_SECOND +
			(time->endTime.tv_usec - time->beginTime.tv_usec));
}

static inline uint64
gp_get_elapsed_ms(GpMonotonicTime * time)
{
	return gp_get_elapsed_us(time) / (USECS_PER_SECOND / MSECS_PER_SECOND);
}

int
GetMaxTupleChunkSizeTCP(void)
{
	return Gp_max_packet_size - PACKET_HEADER_SIZE - TUPLE_CHUNK_HEADER_SIZE;
}

int32
GetListenPortTCP(void)
{
	return tcp_listener_port;
}

void
WaitInterconnectQuitTCP(void)
{
	/* do nothing */
}
