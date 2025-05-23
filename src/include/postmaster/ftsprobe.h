/*-------------------------------------------------------------------------
 *
 * ftsprobe.h
 *	  Interface for fault tolerance service Sender.
 *
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/postmaster/ftsprobe.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FTSPROBE_H
#define FTSPROBE_H
#include "access/xlogdefs.h"

typedef struct
{
	int16 dbid;
	bool isPrimaryAlive;
	bool isMirrorAlive;
	bool isInSync;
	bool isSyncRepEnabled;
	bool isRoleMirror;
	bool retryRequested;
} fts_result;

/* States used by FTS main loop for probing segments. */
typedef enum
{
	FTS_PROBE_SEGMENT,         /* send probe message */
	FTS_SYNCREP_OFF_SEGMENT,   /* turn off syncrep due to mirror down */
	FTS_PROMOTE_SEGMENT,       /* promote a mirror due to primary down */

	/* wait before making another retry attempt */
	FTS_PROBE_RETRY_WAIT,
	FTS_SYNCREP_OFF_RETRY_WAIT,
	FTS_PROMOTE_RETRY_WAIT,

	FTS_PROBE_SUCCESS,         /* response to probe is ready for processing */
	FTS_SYNCREP_OFF_SUCCESS,   /* syncrep was turned off by the primary */
	FTS_PROMOTE_SUCCESS,       /* promotion was triggered on the mirror */

	FTS_PROBE_FAILED,          /* the segment should be considered down */

	FTS_SYNCREP_OFF_FAILED,    /*
								* let the next probe cycle find out what
								* happened to the primary
								*/
	FTS_PROMOTE_FAILED,        /* double fault */

	FTS_RESPONSE_PROCESSED     /*
								* final state, nothing more needs to be done in
								* this probe cycle
								*/
} FtsMessageState;

/* States indicating what status PM is in restarting. */
typedef enum PMRestartState
{
	PM_NOT_IN_RESTART, /* PM is not restarting */
	PM_IN_RESETTING, /* PM is in resetting */
	PM_IN_RECOVERY_MAKING_PROGRESS, /* PM is in recovery and is making progress */
	PM_IN_RECOVERY_NOT_MAKING_PROGRESS /* PM is in recovery but not making progress*/
} PMRestartState;

#define IsFtsMessageStateSuccess(state) (state == FTS_PROBE_SUCCESS || \
		state == FTS_SYNCREP_OFF_SUCCESS || state == FTS_PROMOTE_SUCCESS)
#define IsFtsMessageStateFailed(state) (state == FTS_PROBE_FAILED || \
		state == FTS_SYNCREP_OFF_FAILED || state == FTS_PROMOTE_FAILED)
typedef struct
{
	/*
	 * The primary_cdbinfo and mirror_cdbinfo are references to primary and
	 * mirror configuration at the beginning of a probe cycle.  They are used
	 * to start libpq connection to send a FTS message.  Their state/role/mode
	 * is not used and does remain unchanged even when configuration is updated
	 * in the middle of a probe cycle (e.g. mirror marked down in configuration
	 * before sending SYNCREP_OFF message).
	 */
	CdbComponentDatabaseInfo *primary_cdbinfo;
	CdbComponentDatabaseInfo *mirror_cdbinfo;
#ifndef USE_INTERNAL_FTS
    bool has_mirror_configured;
#endif
	fts_result result;
	FtsMessageState state;
	short poll_events;
	short poll_revents;
	int16 fd_index;               /* index into PollFds array */
	pg_time_t startTime;          /* probe start timestamp */
	pg_time_t retryStartTime;     /* time at which next retry attempt can start */
	int16 probe_errno;            /* saved errno from the latest system call */
	struct pg_conn *conn;         /* libpq connection object */
	int retry_count;
	XLogRecPtr xlogrecptr;
	PMRestartState restart_state;
	/*
	 * How many times of time error (reported by poll()) we have got
	 * when gp_fts_probe_timeout is exceeded
	 *
	 * Say by default the timeout parameter passed to poll() is 50 ms
	 * (see ftsPoll()), and gp_fts_probe_timeout is 20 sec. If we hit
	 * gp_fts_probe_timeout:
	 * 1. If the congestion is on network or segs, timeout_count should
	 *    be near to 20000 / 50 = 400.
	 * 2. If timeout_count is much less than 400, it means that FTS did
	 *    not get enough CPU chance and the congestion must be on master.
	 */
	int timeout_count;
} fts_segment_info;

#ifdef USE_INTERNAL_FTS
typedef struct
{
	int num_pairs; /* number of primary-mirror pairs FTS wants to probe */

	fts_segment_info *perSegInfos;
} fts_context;


extern bool FtsWalRepMessageSegments(CdbComponentDatabases *context);
#endif  /* USE_INTERNAL_FTS */

#endif