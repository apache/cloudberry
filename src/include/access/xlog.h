/*
 * xlog.h
 *
 * PostgreSQL write-ahead log manager
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/xlog.h
 */
#ifndef XLOG_H
#define XLOG_H

#include "postgres.h" /* for Datum */

#include "access/rmgr.h"
#include "access/xlogdefs.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "common/kmgr_utils.h"
#include "datatype/timestamp.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"
#include "lib/stringinfo.h"
#include "storage/buf.h"
#include "storage/fd.h"
#include "utils/pg_crc.h"
#include "utils/relcache.h"
#include "cdb/cdbpublic.h"
#include "nodes/pg_list.h"


/* Sync methods */
#define SYNC_METHOD_FSYNC		0
#define SYNC_METHOD_FDATASYNC	1
#define SYNC_METHOD_OPEN		2	/* for O_SYNC */
#define SYNC_METHOD_FSYNC_WRITETHROUGH	3
#define SYNC_METHOD_OPEN_DSYNC	4	/* for O_DSYNC */
extern int	sync_method;

extern PGDLLIMPORT TimeLineID ThisTimeLineID;	/* current TLI */

/*
 * Prior to 8.4, all activity during recovery was carried out by the startup
 * process. This local variable continues to be used in many parts of the
 * code to indicate actions taken by RecoveryManagers. Other processes that
 * potentially perform work during recovery should check RecoveryInProgress().
 * See XLogCtl notes in xlog.c.
 */
extern bool InRecovery;

/*
 * Like InRecovery, standbyState is only valid in the startup process.
 * In all other processes it will have the value STANDBY_DISABLED (so
 * InHotStandby will read as false).
 *
 * In DISABLED state, we're performing crash recovery or hot standby was
 * disabled in postgresql.conf.
 *
 * In INITIALIZED state, we've run InitRecoveryTransactionEnvironment, but
 * we haven't yet processed a RUNNING_XACTS or shutdown-checkpoint WAL record
 * to initialize our primary-transaction tracking system.
 *
 * When the transaction tracking is initialized, we enter the SNAPSHOT_PENDING
 * state. The tracked information might still be incomplete, so we can't allow
 * connections yet, but redo functions must update the in-memory state when
 * appropriate.
 *
 * In SNAPSHOT_READY mode, we have full knowledge of transactions that are
 * (or were) running on the primary at the current WAL location. Snapshots
 * can be taken, and read-only queries can be run.
 */
typedef enum
{
	STANDBY_DISABLED,
	STANDBY_INITIALIZED,
	STANDBY_SNAPSHOT_PENDING,
	STANDBY_SNAPSHOT_READY
} HotStandbyState;

extern HotStandbyState standbyState;

#define InHotStandby (standbyState >= STANDBY_SNAPSHOT_PENDING)

/*
 * Recovery target type.
 * Only set during a Point in Time recovery, not when in standby mode.
 */
typedef enum
{
	RECOVERY_TARGET_UNSET,
	RECOVERY_TARGET_XID,
	RECOVERY_TARGET_TIME,
	RECOVERY_TARGET_NAME,
	RECOVERY_TARGET_LSN,
	RECOVERY_TARGET_IMMEDIATE
} RecoveryTargetType;

/*
 * Recovery target TimeLine goal
 */
typedef enum
{
	RECOVERY_TARGET_TIMELINE_CONTROLFILE,
	RECOVERY_TARGET_TIMELINE_LATEST,
	RECOVERY_TARGET_TIMELINE_NUMERIC
} RecoveryTargetTimeLineGoal;

extern XLogRecPtr ProcLastRecPtr;
extern XLogRecPtr XactLastRecEnd;
extern PGDLLIMPORT XLogRecPtr XactLastCommitEnd;

extern bool reachedConsistency;

/* these variables are GUC parameters related to XLOG */
extern int	wal_segment_size;
extern int	min_wal_size_mb;
extern int	max_wal_size_mb;
extern int	wal_keep_size_mb;
extern int	max_slot_wal_keep_size_mb;
extern int	XLOGbuffers;
extern int	XLogArchiveTimeout;
extern int	wal_retrieve_retry_interval;
extern char *XLogArchiveCommand;
extern bool EnableHotStandby;

extern bool fullPageWrites;
extern bool wal_log_hints;
extern bool wal_compression;
extern bool wal_init_zero;
extern bool wal_recycle;
extern bool *wal_consistency_checking;
extern char *wal_consistency_checking_string;
extern bool log_checkpoints;
extern char *recoveryRestoreCommand;
extern char *recoveryEndCommand;
extern char *archiveCleanupCommand;
extern bool recoveryTargetInclusive;
extern int	recoveryTargetAction;
extern int	recovery_min_apply_delay;
extern char *PrimaryConnInfo;
extern char *PrimarySlotName;
extern bool wal_receiver_create_temp_slot;
extern bool track_wal_io_timing;

/* indirectly set via GUC system */
extern TransactionId recoveryTargetXid;
extern char *recovery_target_time_string;
extern const char *recoveryTargetName;
extern XLogRecPtr recoveryTargetLSN;
extern RecoveryTargetType recoveryTarget;
extern char *PromoteTriggerFile;
extern RecoveryTargetTimeLineGoal recoveryTargetTimeLineGoal;
extern TimeLineID recoveryTargetTLIRequested;
extern TimeLineID recoveryTargetTLI;

extern int	CheckPointSegments;

/* option set locally in startup process only when signal files exist */
extern bool StandbyModeRequested;
extern bool StandbyMode;

/* tde feature enable or not */
extern int  FileEncryptionEnabled;

/* Hook for plugins to do some startup job */
typedef void (*Startup_hook_type) (void);
extern PGDLLIMPORT Startup_hook_type Startup_hook;

/* Hook for extensions to do consistency check */
typedef void (*ConsistencyCheck_hook_type) (void);
extern PGDLLIMPORT ConsistencyCheck_hook_type xlog_check_consistency_hook;

/* Hook for extensions to do drop database xlog record
 *
 * For custom wal log, when mirror redo xlog, since each restart will start
 * replaying from the latest checkpoint's REDO location in pg_control, when
 * processing the xlog of data writing, the entire db directory may be deleted
 * due to the drop database xlog that has been replayed last time. We need to
 * provide a hook for the implementation of custom wal log to handle this situation.
 *
 * Why can't forget_invalid_pages_db meet the requirements?
 * Because forget_invalid_pages_db limits the table to be organized in page mode.
 * For custom Table-Am, their data organization is not necessarily in page mode.
 */
typedef void (*XLOGDropDatabase_hook_type)(Oid dbid);
extern XLOGDropDatabase_hook_type XLOGDropDatabase_hook;

/* Archive modes */
typedef enum ArchiveMode
{
	ARCHIVE_MODE_OFF = 0,		/* disabled */
	ARCHIVE_MODE_ON,			/* enabled while server is running normally */
	ARCHIVE_MODE_ALWAYS			/* enabled always (even during recovery) */
} ArchiveMode;
extern int	XLogArchiveMode;

/* WAL levels */
typedef enum WalLevel
{
	WAL_LEVEL_MINIMAL = 0,
	WAL_LEVEL_REPLICA,
	WAL_LEVEL_LOGICAL
} WalLevel;

/* Recovery states */
typedef enum RecoveryState
{
	RECOVERY_STATE_CRASH = 0,	/* crash recovery */
	RECOVERY_STATE_ARCHIVE,		/* archive recovery */
	RECOVERY_STATE_DONE			/* currently in production */
} RecoveryState;

/* Recovery pause states */
typedef enum RecoveryPauseState
{
	RECOVERY_NOT_PAUSED,		/* pause not requested */
	RECOVERY_PAUSE_REQUESTED,	/* pause requested, but not yet paused */
	RECOVERY_PAUSED				/* recovery is paused */
} RecoveryPauseState;

extern PGDLLIMPORT int wal_level;

/* Is WAL archiving enabled (always or only while server is running normally)? */
#define XLogArchivingActive() \
	(AssertMacro(XLogArchiveMode == ARCHIVE_MODE_OFF || wal_level >= WAL_LEVEL_REPLICA), XLogArchiveMode > ARCHIVE_MODE_OFF)
/* Is WAL archiving enabled always (even during recovery)? */
#define XLogArchivingAlways() \
	(AssertMacro(XLogArchiveMode == ARCHIVE_MODE_OFF || wal_level >= WAL_LEVEL_REPLICA), XLogArchiveMode == ARCHIVE_MODE_ALWAYS)
#define XLogArchiveCommandSet() (XLogArchiveCommand[0] != '\0')

/*
 * Is WAL-logging necessary for archival or log-shipping, or can we skip
 * WAL-logging if we fsync() the data before committing instead?
 */
#define XLogIsNeeded() (wal_level >= WAL_LEVEL_REPLICA)

/*
 * Is a full-page image needed for hint bit updates?
 *
 * Normally, we don't WAL-log hint bit updates, but if checksums or encryption
 * is enabled, we have to protect them against torn page writes.  When you
 * only set individual bits on a page, it's still consistent no matter what
 * combination of the bits make it to disk, but the checksum wouldn't match.
 * Cluster file encryption requires a new LSN for hint bit changes, and can't
 * tolerate torn pages.  Also WAL-log them if forced by wal_log_hints=on.
 */
#define XLogHintBitIsNeeded() \
		(DataChecksumsEnabled() || FileEncryptionEnabled || wal_log_hints)

/* Do we need to WAL-log information required only for Hot Standby and logical replication? */
#define XLogStandbyInfoActive() (wal_level >= WAL_LEVEL_REPLICA)

/* Do we need to WAL-log information required only for logical replication? */
#define XLogLogicalInfoActive() (wal_level >= WAL_LEVEL_LOGICAL)

#ifdef WAL_DEBUG
extern bool XLOG_DEBUG;
#endif

/*
 * OR-able request flag bits for checkpoints.  The "cause" bits are used only
 * for logging purposes.  Note: the flags must be defined so that it's
 * sensible to OR together request flags arising from different requestors.
 */

/* These directly affect the behavior of CreateCheckPoint and subsidiaries */
#define CHECKPOINT_IS_SHUTDOWN	0x0001	/* Checkpoint is for shutdown */
#define CHECKPOINT_END_OF_RECOVERY	0x0002	/* Like shutdown checkpoint, but
											 * issued at end of WAL recovery */
#define CHECKPOINT_IMMEDIATE	0x0004	/* Do it without delays */
#define CHECKPOINT_FORCE		0x0008	/* Force even if no activity */
#define CHECKPOINT_FLUSH_ALL	0x0010	/* Flush all pages, including those
										 * belonging to unlogged tables */
/* These are important to RequestCheckpoint */
#define CHECKPOINT_WAIT			0x0020	/* Wait for completion */
#define CHECKPOINT_REQUESTED	0x0040	/* Checkpoint request has been made */
/* These indicate the cause of a checkpoint request */
#define CHECKPOINT_CAUSE_XLOG	0x0080	/* XLOG consumption */
#define CHECKPOINT_CAUSE_TIME	0x0100	/* Elapsed time */

/*
 * Flag bits for the record being inserted, set using XLogSetRecordFlags().
 */
#define XLOG_INCLUDE_ORIGIN		0x01	/* include the replication origin */
#define XLOG_MARK_UNIMPORTANT	0x02	/* record not important for durability */
#define XLOG_INCLUDE_XID		0x04	/* WAL-internal message-passing hack */


/* Checkpoint statistics */
typedef struct CheckpointStatsData
{
	TimestampTz ckpt_start_t;	/* start of checkpoint */
	TimestampTz ckpt_write_t;	/* start of flushing buffers */
	TimestampTz ckpt_sync_t;	/* start of fsyncs */
	TimestampTz ckpt_sync_end_t;	/* end of fsyncs */
	TimestampTz ckpt_end_t;		/* end of checkpoint */

	int			ckpt_bufs_written;	/* # of buffers written */

	int			ckpt_segs_added;	/* # of new xlog segments created */
	int			ckpt_segs_removed;	/* # of xlog segments deleted */
	int			ckpt_segs_recycled; /* # of xlog segments recycled */

	int			ckpt_sync_rels; /* # of relations synced */
	uint64		ckpt_longest_sync;	/* Longest sync for one relation */
	uint64		ckpt_agg_sync_time; /* The sum of all the individual sync
									 * times, which is not necessarily the
									 * same as the total elapsed time for the
									 * entire sync phase. */
} CheckpointStatsData;

extern CheckpointStatsData CheckpointStats;

/*
 * GetWALAvailability return codes
 */
typedef enum WALAvailability
{
	WALAVAIL_INVALID_LSN,		/* parameter error */
	WALAVAIL_RESERVED,			/* WAL segment is within max_wal_size */
	WALAVAIL_EXTENDED,			/* WAL segment is reserved by a slot or
								 * wal_keep_size */
	WALAVAIL_UNRESERVED,		/* no longer reserved, but not removed yet */
	WALAVAIL_REMOVED			/* WAL segment has been removed */
} WALAvailability;

struct XLogRecData;

extern XLogRecPtr XLogInsertRecord(struct XLogRecData *rdata,
								   XLogRecPtr fpw_lsn,
								   uint8 flags,
								   int num_fpi);
extern void XLogFlush(XLogRecPtr RecPtr);
extern bool XLogBackgroundFlush(void);
extern bool XLogNeedsFlush(XLogRecPtr RecPtr);
extern int	XLogFileInit(XLogSegNo segno, bool *use_existent, bool use_lock);
extern int	XLogFileOpen(XLogSegNo segno);

extern void CheckXLogRemoved(XLogSegNo segno, TimeLineID tli);
extern XLogSegNo XLogGetLastRemovedSegno(void);
extern void XLogSetAsyncXactLSN(XLogRecPtr record);
extern void XLogSetReplicationSlotMinimumLSN(XLogRecPtr lsn);

extern void xlog_redo(XLogReaderState *record);
extern void xlog_desc(StringInfo buf, XLogReaderState *record);
extern const char *xlog_identify(uint8 info);
extern void UnpackCheckPointRecord(struct XLogReaderState *record, CheckpointExtendedRecord *ckptExtended);

extern void issue_xlog_fsync(int fd, XLogSegNo segno);

extern bool RecoveryInProgress(void);
extern RecoveryState GetRecoveryState(void);
extern bool HotStandbyActive(void);
extern bool HotStandbyActiveInReplay(void);
extern bool XLogInsertAllowed(void);
extern void GetXLogReceiptTime(TimestampTz *rtime, bool *fromStream);
extern XLogRecPtr GetXLogReplayRecPtr(TimeLineID *replayTLI);
extern XLogRecPtr GetXLogInsertRecPtr(void);
extern XLogRecPtr GetXLogWriteRecPtr(void);
extern RecoveryPauseState GetRecoveryPauseState(void);
extern void SetRecoveryPause(bool recoveryPause);
extern TimestampTz GetLatestXTime(void);
extern TimestampTz GetCurrentChunkReplayStartTime(void);

extern void UpdateControlFile(void);
extern uint64 GetSystemIdentifier(void);
extern char *GetMockAuthenticationNonce(void);
extern bool DataChecksumsEnabled(void);

extern int GetFileEncryptionMethod(void);

extern XLogRecPtr GetFakeLSNForUnloggedRel(void);
extern Size XLOGShmemSize(void);
extern void XLOGShmemInit(void);
extern void BootStrapXLOG(void);
extern void LocalProcessControlFile(bool reset);
extern void StartupXLOG(void);
extern void ShutdownXLOG(int code, Datum arg);
extern void InitXLOGAccess(void);
extern void CreateCheckPoint(int flags);
extern bool CreateRestartPoint(int flags);
extern WALAvailability GetWALAvailability(XLogRecPtr targetLSN);
extern XLogRecPtr CalculateMaxmumSafeLSN(void);
extern void XLogPutNextOid(Oid nextOid);
extern void XLogPutNextRelfilenode(Oid nextRelfilenode);
extern void XLogPutNextGxid(DistributedTransactionId nextGxid);
extern XLogRecPtr XLogRestorePoint(const char *rpName);
extern void UpdateFullPageWrites(void);
extern void GetFullPageWriteInfo(XLogRecPtr *RedoRecPtr_p, bool *doPageWrites_p);
extern XLogRecPtr GetRedoRecPtr(void);
extern XLogRecPtr GetInsertRecPtr(void);
extern XLogRecPtr GetFlushRecPtr(void);
extern XLogRecPtr GetLastImportantRecPtr(void);
extern void RemovePromoteSignalFiles(void);

extern void HandleStartupProcInterrupts(void);
extern void StartupProcessMain(void);
extern bool PromoteIsTriggered(void);
extern bool CheckPromoteSignal(void);
extern void WakeupRecovery(void);
extern void SetWalWriterSleeping(bool sleeping);

extern void StartupRequestWalReceiverRestart(void);
extern void XLogRequestWalReceiverReply(void);

extern void assign_max_wal_size(int newval, void *extra);
extern void assign_checkpoint_completion_target(double newval, void *extra);
/*
 * Routines to start, stop, and get status of a base backup.
 */

/*
 * Session-level status of base backups
 *
 * This is used in parallel with the shared memory status to control parallel
 * execution of base backup functions for a given session, be it a backend
 * dedicated to replication or a normal backend connected to a database. The
 * update of the session-level status happens at the same time as the shared
 * memory counters to keep a consistent global and local state of the backups
 * running.
 */
typedef enum SessionBackupState
{
	SESSION_BACKUP_NONE,
	SESSION_BACKUP_EXCLUSIVE,
	SESSION_BACKUP_NON_EXCLUSIVE
} SessionBackupState;

extern XLogRecPtr do_pg_start_backup(const char *backupidstr, bool fast,
									 TimeLineID *starttli_p, StringInfo labelfile,
									 List **tablespaces, StringInfo tblspcmapfile);
extern XLogRecPtr do_pg_stop_backup(char *labelfile, bool waitforarchive,
									TimeLineID *stoptli_p);
extern void do_pg_abort_backup(int code, Datum arg);
extern void register_persistent_abort_backup_handler(void);
extern SessionBackupState get_backup_status(void);

/* File path names (all relative to $PGDATA) */
#define RECOVERY_SIGNAL_FILE	"recovery.signal"
#define STANDBY_SIGNAL_FILE		"standby.signal"
#define BACKUP_LABEL_FILE		"backup_label"
#define BACKUP_LABEL_OLD		"backup_label.old"

#define TABLESPACE_MAP			"tablespace_map"
#define TABLESPACE_MAP_OLD		"tablespace_map.old"

/* files to signal promotion to primary */
#define PROMOTE_SIGNAL_FILE		"promote"

/* Cloudberry additions */
extern bool IsCrashRecoveryOnly(void);
extern DBState GetCurrentDBState(void);
extern XLogRecPtr last_xlog_replay_location(void);
extern void wait_for_mirror(void);
extern void wait_to_avoid_large_repl_lag(void);
extern bool IsRoleMirror(void);
extern void SignalPromote(void);
extern XLogRecPtr XLogLastInsertBeginLoc(void);
extern void initialize_wal_bytes_written(void);

#endif							/* XLOG_H */
