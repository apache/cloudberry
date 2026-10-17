/*-------------------------------------------------------------------------
 *
 * libpq_isbusy_notify.c
 *		PQisBusy() does not report a notification that is ready to collect.
 *
 * libpq's parser handles a NOTIFY by queueing it on the connection and
 * carrying on: pqParseInput3's 'A' arm calls getNotify() and then breaks,
 * never touching asyncStatus.  PQisBusy() reports asyncStatus, so while the
 * command that is in flight has not finished it keeps answering true -- even
 * though a complete notification is sitting in conn->notifyHead with nothing
 * left unread in the connection's buffer.
 *
 * Anything that treats !PQisBusy() as "nothing is ready for me" therefore has
 * a blind spot, and the blind spot bites when the bytes reached the buffer
 * without the socket staying readable.  That is not exotic: a write that
 * cannot complete in one go reads the peer's pending output to avoid
 * deadlocking against it (pqSendSome -> pqReadData, fe-misc.c), so the
 * notification can already be in our memory while poll() has nothing to
 * report.  The coordinator's dispatcher hit exactly this, which is why it now
 * drains notifications before deciding whether to wait
 * (checkDispatchResult, cdbdisp_async.c).
 *
 * This test pins the libpq behaviour that reasoning depends on.  No server is
 * involved and nothing is read from a socket: the input buffer is filled by
 * hand with one complete NOTIFY message, which models the state after the
 * bytes have already been absorbed.  If libpq ever changes so that a queued
 * notification stops PQisBusy() returning true, this fails -- and the drain in
 * the dispatcher has become unnecessary.
 *
 * IDENTIFICATION
 *		src/test/modules/libpq_isbusy_notify/libpq_isbusy_notify.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <arpa/inet.h>

#include "libpq-fe.h"
#include "libpq-int.h"

/* A NOTIFY as it appears on the wire, minus the leading message type byte. */
static const char notify_channel[] = "anser";
static const char notify_payload[] = "part";

int
main(void)
{
	PGconn	   *conn;
	uint32		netlen;
	uint32		netpid;
	int			msglen;
	int			pos = 0;
	int			busy;
	int			unread;
	PGnotify   *notify;
	int			failures = 0;

	/*
	 * PQconnectStart() allocates the buffers and returns without waiting for
	 * the connection, which is all we need -- the path is deliberately one
	 * that cannot exist, and the resulting status is overwritten below.  We
	 * never read or write the socket, so no server is required.
	 */
	conn = PQconnectStart("host=/nonexistent-libpq-isbusy-notify dbname=postgres");
	if (conn == NULL)
	{
		fprintf(stderr, "could not allocate a connection\n");
		return 2;
	}

	/* 4-byte length, 4-byte sender pid, then two NUL-terminated strings. */
	msglen = 4 + 4 + sizeof(notify_channel) + sizeof(notify_payload);
	if (conn->inBufSize < msglen + 1)
	{
		fprintf(stderr, "input buffer is too small for the test message\n");
		PQfinish(conn);
		return 2;
	}

	/*
	 * Put the connection in the state it would be in with a command still in
	 * flight and one complete notification already parsed off the socket.
	 */
	conn->status = CONNECTION_OK;
	conn->asyncStatus = PGASYNC_BUSY;
	conn->inStart = conn->inCursor = 0;

	conn->inBuffer[pos++] = 'A';
	netlen = htonl(msglen);
	memcpy(conn->inBuffer + pos, &netlen, 4);
	pos += 4;
	netpid = htonl(123);
	memcpy(conn->inBuffer + pos, &netpid, 4);
	pos += 4;
	memcpy(conn->inBuffer + pos, notify_channel, sizeof(notify_channel));
	pos += sizeof(notify_channel);
	memcpy(conn->inBuffer + pos, notify_payload, sizeof(notify_payload));
	pos += sizeof(notify_payload);
	conn->inEnd = pos;

	/* Parses the buffer; never touches the socket. */
	busy = PQisBusy(conn);
	unread = conn->inEnd - conn->inStart;

	printf("PQisBusy=%d notify_queued=%d unread_bytes=%d\n",
		   busy, conn->notifyHead != NULL, unread);

	/*
	 * The three halves of the trap, each worth failing on separately: the
	 * notification is ready, the connection still reports itself busy, and
	 * there is nothing left that would make the socket readable again.
	 */
	if (conn->notifyHead == NULL)
	{
		fprintf(stderr, "FAIL: the notification was not queued\n");
		failures++;
	}
	if (!busy)
	{
		fprintf(stderr,
				"FAIL: PQisBusy() reported the notification, so a caller "
				"watching it alone would not miss one\n");
		failures++;
	}
	if (unread != 0)
	{
		fprintf(stderr,
				"FAIL: %d byte(s) left unparsed, so the message was not "
				"complete and the test proved nothing\n", unread);
		failures++;
	}

	/* And it really is collectable, by the call that does look for it. */
	notify = PQnotifies(conn);
	if (notify == NULL)
	{
		fprintf(stderr, "FAIL: PQnotifies() returned nothing\n");
		failures++;
	}
	else
	{
		printf("PQnotifies: channel=%s payload=%s\n",
			   notify->relname, notify->extra);
		if (strcmp(notify->relname, notify_channel) != 0 ||
			strcmp(notify->extra, notify_payload) != 0)
		{
			fprintf(stderr, "FAIL: the notification did not round-trip\n");
			failures++;
		}
		PQfreemem(notify);
	}

	/*
	 * Nothing was ever connected, so stop PQfinish() from trying to send a
	 * terminate message down a socket we invented a state for.
	 */
	conn->status = CONNECTION_BAD;
	PQfinish(conn);

	return failures == 0 ? 0 : 1;
}
