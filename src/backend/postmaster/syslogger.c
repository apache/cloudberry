/*-------------------------------------------------------------------------
 *
 * syslogger.c
 *
 * The system logger (syslogger) appeared in Postgres 8.0. It catches all
 * stderr output from the postmaster, backends, and other subprocesses
 * by redirecting to a pipe, and writes it to a set of logfiles.
 * It's possible to have size and age limits for the logfile configured
 * in postgresql.conf. If these limits are reached or passed, the
 * current logfile is closed and a new one is created (rotated).
 * The logfiles are stored in a subdirectory (configurable in
 * postgresql.conf), using a user-selectable naming scheme.
 *
 * Author: Andreas Pflug <pgadmin@pse-consulting.de>
 *
 * Copyright (c) 2004-2021, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/syslogger.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "common/file_perm.h"
#include "lib/stringinfo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "pgtime.h"
#include "postmaster/fork_process.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "postmaster/syslogger.h"
#include "storage/dsm.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pg_shmem.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"

#include "cdb/cdbvars.h"

/* The maximum bytes for error message */
#define ERROR_MESSAGE_MAX_SIZE 200

/*
 * We read() into a temp buffer twice as big as a chunk, so that any fragment
 * left after processing can be moved down to the front and we'll still have
 * room to read a full chunk.
 */
#define READ_BUF_SIZE (2 * PIPE_CHUNK_SIZE)

/* Log rotation signal file path, relative to $PGDATA */
#define LOGROTATE_SIGNAL_FILE	"logrotate"


/*
 * GUC parameters.  Logging_collector cannot be changed after postmaster
 * start, but the rest can change at SIGHUP.
 */
bool		Logging_collector = false;
int			Log_RotationAge = HOURS_PER_DAY * MINS_PER_HOUR;
int			Log_RotationSize = 10 * 1024;
char	   *Log_directory = NULL;
char	   *Log_filename = NULL;
bool		Log_truncate_on_rotation = false;
int			Log_file_mode = S_IRUSR | S_IWUSR;
int         gp_log_format = 0; /* Text format */

extern bool redirection_done;

/*
 * Private state
 */
static pg_time_t next_rotation_time;
static bool pipe_eof_seen = false;
static bool rotation_disabled = false;
static FILE *syslogFile = NULL;
static FILE *csvlogFile = NULL;
NON_EXEC_STATIC pg_time_t first_syslogger_file_time = 0;
static char *last_file_name = NULL;
static char *last_csv_file_name = NULL;

/*
 * Buffers for saving partial messages from different backends.
 *
 * Keep NBUFFER_LISTS lists of these, with the entry for a given source pid
 * being in the list numbered (pid % NBUFFER_LISTS), so as to cut down on
 * the number of entries we have to examine for any one incoming message.
 * There must never be more than one entry for the same source pid.
 *
 * An inactive buffer is not removed from its list, just held for re-use.
 * An inactive buffer has pid == 0 and undefined contents of data.
 */
typedef struct
{
	int32		pid;			/* PID of source process */
	int32		tid;			/* thread id of source thread */
	StringInfoData data;		/* accumulated data, as a StringInfo */
} save_buffer;

#define NBUFFER_LISTS 256
static List *buffer_lists[NBUFFER_LISTS];

/* These must be exported for EXEC_BACKEND case ... annoying */
#ifndef WIN32
int			syslogPipe[2] = {-1, -1};
#else
HANDLE		syslogPipe[2] = {0, 0};
#endif

#ifdef WIN32
static HANDLE threadHandle = 0;
static CRITICAL_SECTION sysloggerSection;
#endif

/* GPDB: wrapper function to silence unused result warning */
static inline void
ignore_returned_result(long long int result)
{
	(void) result;
}

static bool chunk_is_postgres_chunk(PipeProtoHeader *hdr)
{
    return hdr->zero == 0 && hdr->pid != 0 && hdr->thid != 0 &&
		(hdr->log_format == 't' || hdr->log_format == 'c') &&
		(hdr->is_last == 't' || hdr->is_last == 'f');
}

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
static volatile sig_atomic_t rotation_requested = false;


/* Local subroutines */
#ifdef EXEC_BACKEND
static pid_t syslogger_forkexec(void);
static void syslogger_parseArgs(int argc, char *argv[]);
#endif
NON_EXEC_STATIC void SysLoggerMain(int argc, char *argv[]) pg_attribute_noreturn();
static void process_pipe_input(char *logbuffer, int *bytes_in_logbuffer);
static void flush_pipe_input(char *logbuffer, int *bytes_in_logbuffer);
static FILE *logfile_open(const char *filename, const char *mode,
						  bool allow_errors);

#ifdef WIN32
static unsigned int __stdcall pipeThread(void *arg);
#endif
static bool logfile_rotate(bool time_based_rotation, bool size_based_rotation, const char *suffix,
						   const char *log_directory, const char *log_filename,
                           FILE **fh, char **last_log_file_name);
static char *logfile_getname(pg_time_t timestamp, const char *suffix, const char *log_directory, const char *log_file_pattern);
static void set_next_rotation_time(void);
static void sigUsr1Handler(SIGNAL_ARGS);
static void update_metainfo_datafile(void);

/*
 * Main entry point for syslogger process
 * argc/argv parameters are valid only in EXEC_BACKEND case.
 */
NON_EXEC_STATIC void
SysLoggerMain(int argc, char *argv[])
{
#ifndef WIN32
	char		logbuffer[READ_BUF_SIZE];
	int			bytes_in_logbuffer = 0;
#endif
	char	   *currentLogDir;
	char	   *currentLogFilename;
	int			currentLogRotationAge;
	pg_time_t	now;
	WaitEventSet *wes;

	now = MyStartTime;

#ifdef EXEC_BACKEND
	syslogger_parseArgs(argc, argv);
#endif							/* EXEC_BACKEND */

	MyBackendType = B_LOGGER;
	if (Gp_role == GP_ROLE_DISPATCH)
		init_ps_display("master logger process");
	else
		init_ps_display("logger process");

	/*
	 * If we restarted, our stderr is already redirected into our own input
	 * pipe.  This is of course pretty useless, not to mention that it
	 * interferes with detecting pipe EOF.  Point stderr to /dev/null. This
	 * assumes that all interesting messages generated in the syslogger will
	 * come through elog.c and will be sent to write_syslogger_file.
	 */
	{
		int			fd = open(DEVNULL, O_WRONLY, 0);

		/*
		 * The closes might look redundant, but they are not: we want to be
		 * darn sure the pipe gets closed even if the open failed.  We can
		 * survive running with stderr pointing nowhere, but we can't afford
		 * to have extra pipe input descriptors hanging around.
		 *
		 * As we're just trying to reset these to go to DEVNULL, there's not
		 * much point in checking for failure from the close/dup2 calls here,
		 * if they fail then presumably the file descriptors are closed and
		 * any writes will go into the bitbucket anyway.
		 */
		close(fileno(stdout));
		close(fileno(stderr));
		if (fd != -1)
		{
			(void) dup2(fd, fileno(stdout));
			(void) dup2(fd, fileno(stderr));
			close(fd);
		}
	}

	/*
	 * Syslogger's own stderr can't be the syslogPipe, so set it back to text
	 * mode if we didn't just close it. (It was set to binary in
	 * SubPostmasterMain).
	 */
#ifdef WIN32
	_setmode(_fileno(stderr),_O_TEXT);
#endif

	redirection_done = true;

	/*
	 * Also close our copy of the write end of the pipe.  This is needed to
	 * ensure we can detect pipe EOF correctly.  (But note that in the restart
	 * case, the postmaster already did this.)
	 */
#ifndef WIN32
	if (syslogPipe[1] >= 0)
		close(syslogPipe[1]);
	syslogPipe[1] = -1;
#else
	if (syslogPipe[1])
		CloseHandle(syslogPipe[1]);
	syslogPipe[1] = 0;
#endif

	/*
	 * Properly accept or ignore signals the postmaster might send us
	 *
	 * Note: we ignore all termination signals, and instead exit only when all
	 * upstream processes are gone, to ensure we don't miss any dying gasps of
	 * broken backends...
	 */

	pqsignal(SIGHUP, SignalHandlerForConfigReload); /* set flag to read config
													 * file */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, SIG_IGN);
	pqsignal(SIGQUIT, SIG_IGN);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, sigUsr1Handler);	/* request log rotation */
	pqsignal(SIGUSR2, SIG_IGN);

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);

	PG_SETMASK(&UnBlockSig);

#ifdef WIN32
	/* Fire up separate data transfer thread */
	InitializeCriticalSection(&sysloggerSection);
	EnterCriticalSection(&sysloggerSection);

	threadHandle = (HANDLE) _beginthreadex(NULL, 0, pipeThread, NULL, 0, NULL);
	if (threadHandle == 0)
		elog(FATAL, "could not create syslogger data transfer thread: %m");
#endif							/* WIN32 */

	/*
	 * Remember active logfiles' name(s).  We recompute 'em from the reference
	 * time because passing down just the pg_time_t is a lot cheaper than
	 * passing a whole file path in the EXEC_BACKEND case.
	 */
	last_file_name = logfile_getname(first_syslogger_file_time, NULL, Log_directory, Log_filename);
	if (csvlogFile != NULL)
		last_csv_file_name = logfile_getname(first_syslogger_file_time, ".csv", Log_directory, Log_filename);

	/* remember active logfile parameters */
	currentLogDir = pstrdup(Log_directory);
	currentLogFilename = pstrdup(Log_filename);
	currentLogRotationAge = Log_RotationAge;
	/* set next planned rotation time */
	set_next_rotation_time();
	update_metainfo_datafile();

	/*
	 * Reset whereToSendOutput, as the postmaster will do (but hasn't yet, at
	 * the point where we forked).  This prevents duplicate output of messages
	 * from syslogger itself.
	 */
	whereToSendOutput = DestNone;

	/*
	 * Set up a reusable WaitEventSet object we'll use to wait for our latch,
	 * and (except on Windows) our socket.
	 *
	 * Unlike all other postmaster child processes, we'll ignore postmaster
	 * death because we want to collect final log output from all backends and
	 * then exit last.  We'll do that by running until we see EOF on the
	 * syslog pipe, which implies that all other backends have exited
	 * (including the postmaster).
	 */
	wes = CreateWaitEventSet(CurrentMemoryContext, 2);
	AddWaitEventToSet(wes, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
#ifndef WIN32
	AddWaitEventToSet(wes, WL_SOCKET_READABLE, syslogPipe[0], NULL, NULL);
#endif

	/*
	 * Reset whereToSendOutput, as the postmaster will do (but hasn't yet, at
	 * the point where we forked).  This prevents duplicate output of messages
	 * from syslogger itself.
	 */
	whereToSendOutput = DestNone;

	/* main worker loop */
	for (;;)
	{
		bool		time_based_rotation = false;
		int			size_rotation_for = 0;
		long		cur_timeout;
		WaitEvent	event;

#ifndef WIN32
		int			rc;
#endif

		bool		all_rotations_occurred = false;

		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		/*
		 * Process any requests or signals received recently.
		 */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);

			/*
			 * Check if the log directory or filename pattern changed in
			 * postgresql.conf. If so, force rotation to make sure we're
			 * writing the logfiles in the right place.
			 */
			if (strcmp(Log_directory, currentLogDir) != 0)
			{
				pfree(currentLogDir);
				currentLogDir = pstrdup(Log_directory);
				rotation_requested = true;

				/*
				 * Also, create new directory if not present; ignore errors
				 */
				(void) MakePGDirectory(Log_directory);
			}
			if (strcmp(Log_filename, currentLogFilename) != 0)
			{
				pfree(currentLogFilename);
				currentLogFilename = pstrdup(Log_filename);
				rotation_requested = true;
			}

			/*
			 * Force a rotation if CSVLOG output was just turned on or off and
			 * we need to open or close csvlogFile accordingly.
			 */
			if (((Log_destination & LOG_DESTINATION_CSVLOG) != 0) !=
				(csvlogFile != NULL))
				rotation_requested = true;

			/*
			 * If rotation time parameter changed, reset next rotation time,
			 * but don't immediately force a rotation.
			 */
			if (currentLogRotationAge != Log_RotationAge)
			{
				currentLogRotationAge = Log_RotationAge;
				set_next_rotation_time();
				update_metainfo_datafile();
			}

			/*
			 * If we had a rotation-disabling failure, re-enable rotation
			 * attempts after SIGHUP, and force one immediately.
			 */
			if (rotation_disabled)
			{
				rotation_disabled = false;
				rotation_requested = true;
			}

			/*
			 * Force rewriting last log filename when reloading configuration.
			 * Even if rotation_requested is false, log_destination may have
			 * been changed and we don't want to wait the next file rotation.
			 */
			update_metainfo_datafile();
		}

		if (Log_RotationAge > 0 && !rotation_disabled)
		{
			/* Do a logfile rotation if it's time */
			now = (pg_time_t) time(NULL);
			if (now >= next_rotation_time)
			{
				rotation_requested = time_based_rotation = true;
			}
		}

		if (!rotation_requested && Log_RotationSize > 0 && !rotation_disabled)
		{
			/* Do a rotation if file is too big */
			if (ftell(syslogFile) >= Log_RotationSize * 1024L)
			{
				rotation_requested = true;
				size_rotation_for |= LOG_DESTINATION_STDERR;
			}
			if (csvlogFile != NULL &&
				ftell(csvlogFile) >= Log_RotationSize * 1024L)
			{
				rotation_requested = true;
				size_rotation_for |= LOG_DESTINATION_CSVLOG;
			}
		}

		all_rotations_occurred = rotation_requested;

		if (rotation_requested)
		{
			/*
			 * Force rotation when both values are zero. It means the request
			 * was sent by pg_rotate_logfile() or "pg_ctl logrotate".
			 */
			if (!time_based_rotation && size_rotation_for == 0)
				size_rotation_for = LOG_DESTINATION_STDERR | LOG_DESTINATION_CSVLOG;

			rotation_requested = false;

			all_rotations_occurred &=
				logfile_rotate(time_based_rotation, (size_rotation_for & LOG_DESTINATION_STDERR) != 0,
							   NULL, Log_directory, Log_filename,
							   &syslogFile, &last_file_name);
			all_rotations_occurred &=
				logfile_rotate(time_based_rotation, (size_rotation_for & LOG_DESTINATION_CSVLOG) != 0,
							   ".csv", Log_directory, Log_filename,
							   &csvlogFile, &last_csv_file_name);
		}

		/*
		 * GPDB: only update our rotation timestamp if every log file above was
		 * able to rotate. In upstream, this would have been done as part of
		 * logfile_rotate() itself -- Postgres calls that function once, whereas
		 * we call it (up to) three times.
		 */
		if (all_rotations_occurred)
		{
			set_next_rotation_time();
			update_metainfo_datafile();
		}

		/*
		 * Calculate time till next time-based rotation, so that we don't
		 * sleep longer than that.  We assume the value of "now" obtained
		 * above is still close enough.  Note we can't make this calculation
		 * until after calling logfile_rotate(), since it will advance
		 * next_rotation_time.
		 *
		 * GPDB: logfile_rotate() doesn't advance next_rotation_time; we do that
		 * explicitly above, once all rotations have been successful.
		 *
		 * Also note that we need to beware of overflow in calculation of the
		 * timeout: with large settings of Log_RotationAge, next_rotation_time
		 * could be more than INT_MAX msec in the future.  In that case we'll
		 * wait no more than INT_MAX msec, and try again.
		 */
		if (Log_RotationAge > 0 && !rotation_disabled)
		{
			pg_time_t	delay;

			delay = next_rotation_time - now;
			if (delay > 0)
			{
				if (delay > INT_MAX / 1000)
					delay = INT_MAX / 1000;
				cur_timeout = delay * 1000L;	/* msec */
			}
			else
				cur_timeout = 0;
		}
		else
			cur_timeout = -1L;

		/*
		 * Sleep until there's something to do
		 */
#ifndef WIN32
		rc = WaitEventSetWait(wes, cur_timeout, &event, 1,
							  WAIT_EVENT_SYSLOGGER_MAIN);

		if (rc == 1 && event.events == WL_SOCKET_READABLE)
		{
			int			bytesRead;

			bytesRead = read(syslogPipe[0],
							 logbuffer + bytes_in_logbuffer,
							 sizeof(logbuffer) - bytes_in_logbuffer);
			if (bytesRead < 0)
			{
				if (errno != EINTR)
					ereport(LOG,
							(errcode_for_socket_access(),
							 errmsg("could not read from logger pipe: %m")));
			}
			else if (bytesRead > 0)
			{
				bytes_in_logbuffer += bytesRead;
				process_pipe_input(logbuffer, &bytes_in_logbuffer);
				continue;
			}
			else
			{
				/*
				 * Zero bytes read when select() is saying read-ready means
				 * EOF on the pipe: that is, there are no longer any processes
				 * with the pipe write end open.  Therefore, the postmaster
				 * and all backends are shut down, and we are done.
				 */
				pipe_eof_seen = true;

				/* if there's any data left then force it out now */
				flush_pipe_input(logbuffer, &bytes_in_logbuffer);
			}
		}
#else							/* WIN32 */

		/*
		 * On Windows we leave it to a separate thread to transfer data and
		 * detect pipe EOF.  The main thread just wakes up to handle SIGHUP
		 * and rotation conditions.
		 *
		 * Server code isn't generally thread-safe, so we ensure that only one
		 * of the threads is active at a time by entering the critical section
		 * whenever we're not sleeping.
		 */
		LeaveCriticalSection(&sysloggerSection);

		(void) WaitEventSetWait(wes, cur_timeout, &event, 1,
								WAIT_EVENT_SYSLOGGER_MAIN);

		EnterCriticalSection(&sysloggerSection);
#endif							/* WIN32 */

		if (pipe_eof_seen)
		{
			/*
			 * seeing this message on the real stderr is annoying - so we make
			 * it DEBUG1 to suppress in normal use.
			 */
			ereport(DEBUG1,
					(errmsg_internal("logger shutting down")));

			/*
			 * Normal exit from the syslogger is here.  Note that we
			 * deliberately do not close syslogFile before exiting; this is to
			 * allow for the possibility of elog messages being generated
			 * inside proc_exit.  Regular exit() will take care of flushing
			 * and closing stdio channels.
			 */
			proc_exit(0);
		}
	}
}

/*
 * Postmaster subroutine to start a syslogger subprocess.
 */
int
SysLogger_Start(void)
{
	pid_t		sysloggerPid;
	char	   *filename;

	if (!Logging_collector)
		return 0;

	/*
	 * If first time through, create the pipe which will receive stderr
	 * output.
	 *
	 * If the syslogger crashes and needs to be restarted, we continue to use
	 * the same pipe (indeed must do so, since extant backends will be writing
	 * into that pipe).
	 *
	 * This means the postmaster must continue to hold the read end of the
	 * pipe open, so we can pass it down to the reincarnated syslogger. This
	 * is a bit klugy but we have little choice.
	 *
	 * Also note that we don't bother counting the pipe FDs by calling
	 * Reserve/ReleaseExternalFD.  There's no real need to account for them
	 * accurately in the postmaster or syslogger process, and both ends of the
	 * pipe will wind up closed in all other postmaster children.
	 */
#ifndef WIN32
	if (syslogPipe[0] < 0)
	{
		if (pipe(syslogPipe) < 0)
			ereport(FATAL,
					(errcode_for_socket_access(),
					 errmsg("could not create pipe for syslog: %m")));
	}
#else
	if (!syslogPipe[0])
	{
		SECURITY_ATTRIBUTES sa;

		memset(&sa, 0, sizeof(SECURITY_ATTRIBUTES));
		sa.nLength = sizeof(SECURITY_ATTRIBUTES);
		sa.bInheritHandle = TRUE;

		if (!CreatePipe(&syslogPipe[0], &syslogPipe[1], &sa, 32768))
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not create pipe for syslog: %m")));
	}
#endif

	/*
	 * Create log directory if not present; ignore errors
	 */
	(void) MakePGDirectory(Log_directory);

	/*
	 * The initial logfile is created right in the postmaster, to verify that
	 * the Log_directory is writable.  We save the reference time so that the
	 * syslogger child process can recompute this file name.
	 *
	 * It might look a bit strange to re-do this during a syslogger restart,
	 * but we must do so since the postmaster closed syslogFile after the
	 * previous fork (and remembering that old file wouldn't be right anyway).
	 * Note we always append here, we won't overwrite any existing file.  This
	 * is consistent with the normal rules, because by definition this is not
	 * a time-based rotation.
	 */
	first_syslogger_file_time = time(NULL);

	filename = logfile_getname(first_syslogger_file_time, NULL, Log_directory, Log_filename);

	syslogFile = logfile_open(filename, "a", false);

	pfree(filename);

	/*
	 * Likewise for the initial CSV log file, if that's enabled.  (Note that
	 * we open syslogFile even when only CSV output is nominally enabled,
	 * since some code paths will write to syslogFile anyway.)
	 */
	if (Log_destination & LOG_DESTINATION_CSVLOG)
	{
		filename = logfile_getname(first_syslogger_file_time, ".csv", Log_directory, Log_filename);

		csvlogFile = logfile_open(filename, "a", false);

		pfree(filename);
	}

#ifdef EXEC_BACKEND
	switch ((sysloggerPid = syslogger_forkexec()))
#else
	switch ((sysloggerPid = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork system logger: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(true);

			/* Drop our connection to postmaster's shared memory, as well */
			dsm_detach_all();
			PGSharedMemoryDetach();

			/* do the work */
			SysLoggerMain(0, NULL);
			break;
#endif

		default:
			/* success, in postmaster */

			/* now we redirect stderr, if not done already */
			if (!redirection_done)
			{
#ifdef WIN32
				int			fd;
#endif

				/*
				 * Leave a breadcrumb trail when redirecting, in case the user
				 * forgets that redirection is active and looks only at the
				 * original stderr target file.
				 */
				ereport(LOG,
						(errmsg("redirecting log output to logging collector process"),
						 errhint("Future log output will appear in directory \"%s\".",
								 Log_directory)));

#ifndef WIN32
				fflush(stdout);
				if (dup2(syslogPipe[1], fileno(stdout)) < 0)
					ereport(FATAL,
							(errcode_for_file_access(),
							 errmsg("could not redirect stdout: %m")));
				fflush(stderr);
				if (dup2(syslogPipe[1], fileno(stderr)) < 0)
					ereport(FATAL,
							(errcode_for_file_access(),
							 errmsg("could not redirect stderr: %m")));
				/* Now we are done with the write end of the pipe. */
				close(syslogPipe[1]);
				syslogPipe[1] = -1;
#else

				/*
				 * open the pipe in binary mode and make sure stderr is binary
				 * after it's been dup'ed into, to avoid disturbing the pipe
				 * chunking protocol.
				 */
				fflush(stderr);
				fd = _open_osfhandle((intptr_t) syslogPipe[1],
									 _O_APPEND | _O_BINARY);
				if (dup2(fd, _fileno(stderr)) < 0)
					ereport(FATAL,
							(errcode_for_file_access(),
							 errmsg("could not redirect stderr: %m")));
				close(fd);
				_setmode(_fileno(stderr), _O_BINARY);

				/*
				 * Now we are done with the write end of the pipe.
				 * CloseHandle() must not be called because the preceding
				 * close() closes the underlying handle.
				 */
				syslogPipe[1] = 0;
#endif
				redirection_done = true;
			}

			/* postmaster will never write the file(s); close 'em */
			fclose(syslogFile);
			syslogFile = NULL;
			if (csvlogFile != NULL)
			{
				fclose(csvlogFile);
				csvlogFile = NULL;
			}
			return (int) sysloggerPid;
	}

	/* we should never reach here */
	return 0;
}


#ifdef EXEC_BACKEND

/*
 * syslogger_forkexec() -
 *
 * Format up the arglist for, then fork and exec, a syslogger process
 */
static pid_t
syslogger_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;
	char		filenobuf[32];
	char        alertFilenobuf[32];
	char		csvfilenobuf[32];

	av[ac++] = "postgres";
	av[ac++] = "--forklog";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */

	/* static variables (those not passed by write_backend_variables) */
#ifndef WIN32
	if (syslogFile != NULL)
		snprintf(filenobuf, sizeof(filenobuf), "%d",
				 fileno(syslogFile));
	else
		strcpy(filenobuf, "-1");
#else							/* WIN32 */
	if (syslogFile != NULL)
		snprintf(filenobuf, sizeof(filenobuf), "%ld",
				 (long) _get_osfhandle(_fileno(syslogFile)));
	else
		strcpy(filenobuf, "0");
#endif							/* WIN32 */
	av[ac++] = filenobuf;

#ifndef WIN32
	if (csvlogFile != NULL)
		snprintf(csvfilenobuf, sizeof(csvfilenobuf), "%d",
				 fileno(csvlogFile));
	else
		strcpy(csvfilenobuf, "-1");
#else							/* WIN32 */
	if (csvlogFile != NULL)
		snprintf(csvfilenobuf, sizeof(csvfilenobuf), "%ld",
				 (long) _get_osfhandle(_fileno(csvlogFile)));
	else
		strcpy(csvfilenobuf, "0");
#endif							/* WIN32 */
	av[ac++] = csvfilenobuf;

	av[ac] = NULL;
	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}

/*
 * syslogger_parseArgs() -
 *
 * Extract data from the arglist for exec'ed syslogger process
 */
static void
syslogger_parseArgs(int argc, char *argv[])
{
	int			fd;
	int         alertFd;

	Assert(argc == 5);
	argv += 3;

	/*
	 * Re-open the error output files that were opened by SysLogger_Start().
	 *
	 * We expect this will always succeed, which is too optimistic, but if it
	 * fails there's not a lot we can do to report the problem anyway.  As
	 * coded, we'll just crash on a null pointer dereference after failure...
	 */
#ifndef WIN32
	fd = atoi(*argv++);
	if (fd != -1)
	{
		syslogFile = fdopen(fd, "a");
		setvbuf(syslogFile, NULL, PG_IOLBF, 0);
	}
	fd = atoi(*argv++);
	if (fd != -1)
	{
		csvlogFile = fdopen(fd, "a");
		setvbuf(csvlogFile, NULL, PG_IOLBF, 0);
	}
#else							/* WIN32 */
	fd = atoi(*argv++);
	if (fd != 0)
	{
		fd = _open_osfhandle(fd, _O_APPEND | _O_TEXT);
		if (fd > 0)
		{
			syslogFile = fdopen(fd, "a");
			setvbuf(syslogFile, NULL, PG_IOLBF, 0);
		}
	}
	fd = atoi(*argv++);
	if (fd != 0)
	{
		fd = _open_osfhandle(fd, _O_APPEND | _O_TEXT);
		if (fd > 0)
		{
			csvlogFile = fdopen(fd, "a");
			setvbuf(csvlogFile, NULL, PG_IOLBF, 0);
		}
	}
#endif							/* WIN32 */
}
#endif							/* EXEC_BACKEND */

/*
 * Write a given timestamp to the log file.
 */
void
syslogger_append_timestamp(pg_time_t stamp_time, bool amsyslogger, bool append_comma)
{
    if(stamp_time != 0)
    {
        char strbuf[128];

        pg_strftime(strbuf, sizeof(strbuf),
                /* Win32 timezone names are too long so don't print them */
#ifndef WIN32
                "%Y-%m-%d %H:%M:%S %Z",
#else
                "%Y-%m-%d %H:%M:%S",
#endif
                pg_localtime(&stamp_time, log_timezone));
		if (amsyslogger)
			write_syslogger_file_binary(strbuf, strlen(strbuf), LOG_DESTINATION_STDERR);
		else
			ignore_returned_result(write(fileno(stderr), strbuf, strlen(strbuf)));
    }

    if (append_comma)
	{
		if (amsyslogger)
			write_syslogger_file_binary(",", 1, LOG_DESTINATION_STDERR);
		else
			ignore_returned_result(write(fileno(stderr), ",", 1));
	}
}

/*
 * Write the current timestamp with milliseconds to the syslogger file or
 * stderr.
 *
 * It is not safe to call strftime since it is not async-safe, and it
 * is expensive to call strftime to get timezone everytime, we use
 * pg_strftime, but stick on a fixed timezone (default_timezone)
 * instead a settable timezone as PostgreSQL does, since we want all
 * log messages to have the same time format. See MPP-2591.
 */
void
syslogger_append_current_timestamp(bool amsyslogger)
{
    struct timeval tv;
    pg_time_t	stamp_time;
    char strbuf[128];
    char msbuf[8];

    gettimeofday(&tv, NULL);
    stamp_time = (pg_time_t) tv.tv_sec;

    pg_strftime(strbuf, sizeof(strbuf),
            /* leave room for milliseconds... */
            /* Win32 timezone names are too long so don't print them */
#ifndef WIN32
            "%Y-%m-%d %H:%M:%S        %Z",
#else
            "%Y-%m-%d %H:%M:%S        ",
#endif
            pg_localtime(&stamp_time, log_timezone));

    /* 'paste' milliseconds into place... */
    sprintf(msbuf, ".%06d", (int) (tv.tv_usec));
    strncpy(strbuf + 19, msbuf, 7);

	if (amsyslogger)
	{
		write_syslogger_file_binary(strbuf, strlen(strbuf), LOG_DESTINATION_STDERR);
		write_syslogger_file_binary(",", 1, LOG_DESTINATION_STDERR);
	}
	else
	{
		ignore_returned_result(write(fileno(stderr), strbuf, strlen(strbuf)));
		ignore_returned_result(write(fileno(stderr), ",", 1));
	}
}


/*
 * We use the PostgreSQL defaults for CSV, i.e. quote = escape = '"'
 * If it's NULL, append nothing.
 */
int syslogger_write_str(const char *data, int len, bool amsyslogger, bool csv)
{
    int cnt = 0;

    /* avoid confusing an empty string with NULL */
    if (data == NULL)
        return 0;

    while (cnt < len && data[cnt] != '\0')
    {
        if (csv && data[cnt] == '"')
		{
			if (amsyslogger)
				write_syslogger_file_binary("\"", 1, LOG_DESTINATION_STDERR);
			else
				ignore_returned_result(write(fileno(stderr), "\"", 1));
		}
		
		if (amsyslogger)
			write_syslogger_file_binary(data+cnt, 1, LOG_DESTINATION_STDERR);
		else
			ignore_returned_result(write(fileno(stderr), data+cnt, 1));

        cnt+=1;
    }

    return cnt;
}

void
syslogger_write_int32(bool test0, const char *prefix, int32 i, bool amsyslogger, bool append_comma)
{
    char buf[1024];
    int len;

    if (!test0 || i > 0)
    {
        len = sprintf(buf, "%s%d", prefix, i);
		if (amsyslogger)
			write_syslogger_file_binary(buf, len, LOG_DESTINATION_STDERR);
		else
			ignore_returned_result(write(fileno(stderr), buf, len));
    }
    if (append_comma)
	{
		if (amsyslogger)
			write_syslogger_file_binary(",", 1, LOG_DESTINATION_STDERR);
		else
			ignore_returned_result(write(fileno(stderr), ",", 1));
	}
}

/*
 * setErrorDataFromSegvChunk
 *   Fill in the given error data with the chunk that contains the message
 * sent in a SEGV/BUS/ILL handler.
 */
static void
fillinErrorDataFromSegvChunk(GpErrorData *errorData, PipeProtoChunk *chunk)
{
	Assert(chunk != NULL &&
		   chunk->hdr.is_segv_msg == 't' &&
		   chunk->hdr.is_last == 't');

	GpSegvErrorData *segvData = (GpSegvErrorData *)chunk->data;
	
	errorData->fix_fields.session_start_time = segvData->session_start_time;
	errorData->fix_fields.omit_location = 'f';

	/* This field is always true now. We should remove this eventually. */
	errorData->fix_fields.gp_is_primary = 't';
	errorData->fix_fields.gp_session_id = segvData->gp_session_id;
	errorData->fix_fields.gp_command_count = segvData->gp_command_count;
	errorData->fix_fields.gp_segment_id = segvData->gp_segment_id;
	errorData->fix_fields.slice_id = segvData->slice_id;
	errorData->fix_fields.error_cursor_pos = 0;
	errorData->fix_fields.internal_query_pos = 0;
	errorData->fix_fields.error_fileline = 0;
	errorData->fix_fields.top_trans_id = 0;
	errorData->fix_fields.dist_trans_id = 0;
	errorData->fix_fields.local_trans_id = 0;
	errorData->fix_fields.subtrans_id = 0;

	errorData->username = NULL;
	errorData->databasename = NULL;
	errorData->remote_host = NULL;
	errorData->remote_port = NULL;
	errorData->error_severity = "PANIC";
	errorData->sql_state = "XX000";
	errorData->error_message = palloc0(ERROR_MESSAGE_MAX_SIZE);

	const char *signalName = SegvBusIllName(segvData->signal_num);
	Assert(signalName != NULL);
	snprintf(errorData->error_message, ERROR_MESSAGE_MAX_SIZE,
			 "Unexpected internal error: %s received signal %s",
			 Gp_role == GP_ROLE_DISPATCH ? "Master process" : "Segment process",
			 signalName);
	
	errorData->error_detail = NULL;
	errorData->error_hint = NULL;
	errorData->internal_query = NULL;
	errorData->error_context = NULL;
	errorData->debug_query_string = NULL;
	errorData->error_func_name = NULL;
	errorData->error_filename = NULL;
	errorData->stacktrace = NULL;
	
	if (segvData->frame_depth > 0)
	{
		void *stackAddressArray = (chunk->data + MAXALIGN(sizeof(GpSegvErrorData)));
		void **stackAddresses = stackAddressArray;
		errorData->stacktrace = gp_stacktrace(stackAddresses, segvData->frame_depth);
	}
}

/*
 * freeErrorDataFields
 *   Free the palloc'ed fields inside GpErrorData.
 *
 * This is the counterpart for fillinErrorDataFromSegvChunk. Currently, only error message and
 * stacktrace need to be freed.
 */
static void
freeErrorDataFields(GpErrorData *errorData)
{
	pfree(errorData->error_message);
	
	if (errorData->stacktrace != NULL)
	{
		pfree(errorData->stacktrace);
	}
}

/*
 * syslogger_write_str_with_comma
 *   Write the given string to the log. A comma is appended after the given string.
 *
 * If csv is true, double quotes are added around the string.
 */
static void
syslogger_write_str_with_comma(const char *data, bool amsyslogger, bool csv, bool quote_empty)
{
	if (data != NULL)
	{
		bool is_empty = (data[0] == '\0');
		if (csv && (!is_empty || quote_empty))
		{
			write_syslogger_file_binary("\"", 1, LOG_DESTINATION_STDERR);
		}

		syslogger_write_str(data, strlen(data), amsyslogger, csv);

		if (csv && (!is_empty || quote_empty))
		{
			write_syslogger_file_binary("\"", 1, LOG_DESTINATION_STDERR);
		}
	}
	
	write_syslogger_file_binary(",", 1, LOG_DESTINATION_STDERR);
}

/*
 * syslogger_write_str_end
 *   Write the given string to the log. No comma is appended after the given string.
 *
 * If csv is true, double quotes are added around the string.
 */
static void
syslogger_write_str_end(const char *data, bool amsyslogger, bool csv, bool quote_empty)
{
	if (data != NULL)
	{
		bool is_empty = (data[0] == '\0');
		if (csv && (!is_empty || quote_empty))
		{
			write_syslogger_file_binary("\"", 1, LOG_DESTINATION_STDERR);
		}

		syslogger_write_str(data, strlen(data), amsyslogger, csv);

		if (csv && (!is_empty || quote_empty))
		{
			write_syslogger_file_binary("\"", 1, LOG_DESTINATION_STDERR);
		}
	}
}

/*
 * syslogger_write_errordata
 *   Write the GpErrorData to the log.
 */
static void
syslogger_write_errordata(PipeProtoHeader *chunkHeader, GpErrorData *errorData, bool csv)
{
	syslogger_append_current_timestamp(true);
	
	/* username */
	syslogger_write_str_with_comma(errorData->username, true, csv, true);
	
	/* databasename */
	syslogger_write_str_with_comma(errorData->databasename, true, csv, true);
	
	/* Process id, thread id */
	syslogger_write_int32(false, "p", chunkHeader->pid, true, true);
	syslogger_write_int32(false, "th", chunkHeader->thid, true, true);
	
	/* Remote host */
	syslogger_write_str_with_comma(errorData->remote_host, true, csv, true);
	/* Remote port */
	syslogger_write_str_with_comma(errorData->remote_port, true, csv, true);
	
	/* session start timestamp */
	syslogger_append_timestamp(errorData->fix_fields.session_start_time, true, true);
	
	/* Transaction id */
	syslogger_write_int32(false, "", errorData->fix_fields.top_trans_id, true, true);
	
	/* GPDB specific options. */
	syslogger_write_int32(true, "con", errorData->fix_fields.gp_session_id, true, true); 
	syslogger_write_int32(true, "cmd", errorData->fix_fields.gp_command_count, true, true); 
	syslogger_write_int32(false, errorData->fix_fields.gp_is_primary == 't'? "seg" : "mir", errorData->fix_fields.gp_segment_id,
						  true, true); 
	syslogger_write_int32(true, "slice", errorData->fix_fields.slice_id, true, true); 
	syslogger_write_int32(true, "dx", errorData->fix_fields.dist_trans_id, true, true);
	syslogger_write_int32(true, "x", errorData->fix_fields.local_trans_id, true, true); 
	syslogger_write_int32(true, "sx", errorData->fix_fields.subtrans_id, true, true); 
	
	/* error severity */
	syslogger_write_str_with_comma(errorData->error_severity, true, csv, true);
	/* sql state code */
	syslogger_write_str_with_comma(errorData->sql_state, true, csv, true);
	/* errmsg */
	syslogger_write_str_with_comma(errorData->error_message, true, csv, true);
	/* errdetail */
	syslogger_write_str_with_comma(errorData->error_detail, true, csv, true);
	/* errhint */
	syslogger_write_str_with_comma(errorData->error_hint, true, csv, true);
	/* internal query */
	syslogger_write_str_with_comma(errorData->internal_query, true, csv, true);
	/* internal query pos */
	syslogger_write_int32(true, "", errorData->fix_fields.internal_query_pos, true, true);
	/* err ctxt */
	syslogger_write_str_with_comma(errorData->error_context, true, csv, true);
	/* user query */
	syslogger_write_str_with_comma(errorData->debug_query_string, true, csv, true);
	/* cursor pos */
	syslogger_write_int32(false, "", errorData->fix_fields.error_cursor_pos, true, true); 
	/* func name */
	syslogger_write_str_with_comma(errorData->error_func_name, true, csv, true);
	/* file name */
	syslogger_write_str_with_comma(errorData->error_filename, true, csv, true);
	/* line number */
	syslogger_write_int32(true, "", errorData->fix_fields.error_fileline, true, true);
	/* stack trace */
	if (errorData->stacktrace != NULL)
	{
		if (csv)
		{
			write_syslogger_file_binary("\"", 1, LOG_DESTINATION_STDERR);
		}
		
		syslogger_write_str(errorData->stacktrace, strlen(errorData->stacktrace), true, csv);

		if (csv)
		{
			write_syslogger_file_binary("\"", 1, LOG_DESTINATION_STDERR);
		}
	}
	
	/* EOL */
	write_syslogger_file_binary(LOG_EOL, strlen(LOG_EOL), LOG_DESTINATION_STDERR);
}

/*
 * syslogger_log_segv_chunk
 * Write the chunk for the message sent inside a SEGV/BUS/ILL handler to the log.
 */
static void
syslogger_log_segv_chunk(PipeProtoChunk *chunk)
{
	Assert(chunk->hdr.is_segv_msg == 't' && chunk->hdr.is_last == 't');
	Assert(chunk->hdr.thid == FIXED_THREAD_ID);

	GpErrorData errorData;
	fillinErrorDataFromSegvChunk(&errorData, chunk);
	syslogger_write_errordata(&chunk->hdr, &errorData, chunk->hdr.log_format == 'c');
	freeErrorDataFields(&errorData);
}

static size_t
pg_strnlen(const char *str, size_t maxlen)
{
	const char *p = str;

	while (maxlen-- > 0 && *p)
		p++;
	return p - str;
}

static char *
get_str_from_chunk_data(char *str, int *cursor, int total)
{
	Assert(str != NULL);

	char * out = NULL;
	int left = total - (*cursor);

	/* Check if the string is an empty string */
	if (left > 0 && str[(*cursor)] == '\0')
	{
		(*cursor)++;
		return strdup("");
	}

	int wlen = pg_strnlen(str + (*cursor), left);

	out = malloc(wlen + 1);
	if (!out)
		return NULL;

	memcpy(out, str + (*cursor), wlen);
	out[wlen] = '\0';
	(*cursor) += wlen + 1; // skip to start of next string.

	return out;
}

void syslogger_log_chunk_data(PipeProtoHeader* p, char *data, int len)
{
	if (p->log_format == 't')
	{
		/* text format chunk data always ended with '\0' */
		write_syslogger_file(data, len, LOG_DESTINATION_STDERR);
	}
	else 
	{
		GpErrorData errorData;
		memset(&errorData, 0, sizeof(errorData));
		memcpy(&errorData.fix_fields, data, sizeof(errorData.fix_fields));
		GpErrorDataFixFields *pfixed = &(errorData.fix_fields);

		int cur = sizeof(GpErrorDataFixFields);
		errorData.username = get_str_from_chunk_data(data, &cur, len);
		errorData.databasename = get_str_from_chunk_data(data, &cur, len);
		errorData.remote_host = get_str_from_chunk_data(data, &cur, len);
		errorData.remote_port = get_str_from_chunk_data(data, &cur, len);
		errorData.error_severity = get_str_from_chunk_data(data, &cur, len);
		errorData.sql_state = get_str_from_chunk_data(data, &cur, len);
		errorData.error_message = get_str_from_chunk_data(data, &cur, len);
		errorData.error_detail = get_str_from_chunk_data(data, &cur, len);
		errorData.error_hint = get_str_from_chunk_data(data, &cur, len);
		errorData.internal_query = get_str_from_chunk_data(data, &cur, len);
		errorData.error_context = get_str_from_chunk_data(data, &cur, len);
		errorData.debug_query_string = get_str_from_chunk_data(data, &cur, len);
		errorData.error_func_name = get_str_from_chunk_data(data, &cur, len);
		errorData.error_filename = get_str_from_chunk_data(data, &cur, len);
		errorData.stacktrace = get_str_from_chunk_data(data, &cur, len);

		/*
		* timestamp_with_milliseconds 
		*/
		syslogger_append_current_timestamp(true);

		/* username */
		syslogger_write_str_with_comma(errorData.username, true, true, false);

		/* databasename */
		syslogger_write_str_with_comma(errorData.databasename, true, true, false);

		/* Process id, thread id */
		syslogger_write_int32(false, "p", p->pid, true, true);
		syslogger_write_int32(false, "th", p->thid, true, true);

		/* Remote host */
		syslogger_write_str_with_comma(errorData.remote_host, true, true, false);
		/* Remote port */
		syslogger_write_str_with_comma(errorData.remote_port, true, true, false);

		/* session start timestamp */
		syslogger_append_timestamp(pfixed->session_start_time, true, true);

		/* Transaction id */
		syslogger_write_int32(false, "", pfixed->top_trans_id, true, true);

		/* GPDB specific options. */
		syslogger_write_int32(true, "con", pfixed->gp_session_id, true, true);
		syslogger_write_int32(true, "cmd", pfixed->gp_command_count, true, true);
		syslogger_write_int32(false, pfixed->gp_is_primary == 't'? "seg" : "mir", pfixed->gp_segment_id,
								true, true);
		syslogger_write_int32(true, "slice", pfixed->slice_id, true, true);
		syslogger_write_int32(true, "dx", pfixed->dist_trans_id, true, true);
		syslogger_write_int32(true, "x", pfixed->local_trans_id, true, true);
		syslogger_write_int32(true, "sx", pfixed->subtrans_id, true, true);

		/* error severity */
		syslogger_write_str_with_comma(errorData.error_severity, true, true, false);
		/* sql state code */
		syslogger_write_str_with_comma(errorData.sql_state, true, true, false);
		/* errmsg */
		syslogger_write_str_with_comma(errorData.error_message, true, true, false);
		/* errdetail */
		syslogger_write_str_with_comma(errorData.error_detail, true, true, false);
		/* errhint */
		syslogger_write_str_with_comma(errorData.error_hint, true, true, false);
		/* internal query */
		syslogger_write_str_with_comma(errorData.internal_query, true, true, false);
		/* internal query pos */
		syslogger_write_int32(true, "", pfixed->internal_query_pos, true, true);
		/* err ctxt */
		syslogger_write_str_with_comma(errorData.error_context, true, true, false);
		/* user query */
		syslogger_write_str_with_comma(errorData.debug_query_string, true, true, false);
		/* cursor pos */
		syslogger_write_int32(false, "", pfixed->error_cursor_pos, true, true);
		/* func name */
		syslogger_write_str_with_comma(errorData.error_func_name, true, true, false);
		/* file name */
		syslogger_write_str_with_comma(errorData.error_filename, true, true, false);
		/* line number */
		syslogger_write_int32(true, "", pfixed->error_fileline, true, true);
		/* stack trace */
		syslogger_write_str_end(errorData.stacktrace, true, true, false);

		/* EOL */
		write_syslogger_file_binary(LOG_EOL, strlen(LOG_EOL), LOG_DESTINATION_STDERR);

		free(errorData.stacktrace ); errorData.stacktrace = NULL;
		free((char *)errorData.error_filename ); errorData.error_filename = NULL;
		free((char *)errorData.error_func_name ); errorData.error_func_name = NULL;
		free(errorData.debug_query_string ); errorData.debug_query_string = NULL;
		free(errorData.error_context); errorData.error_context = NULL;
		free(errorData.internal_query ); errorData.internal_query = NULL;
		free(errorData.error_hint ); errorData.error_hint = NULL;
		free(errorData.error_detail ); errorData.error_detail = NULL;
		free(errorData.error_message ); errorData.error_message = NULL;
		free(errorData.sql_state ); errorData.sql_state = NULL;
		free((char *)errorData.error_severity ); errorData.error_severity = NULL;
		free(errorData.remote_port ); errorData.remote_port = NULL;
		free(errorData.remote_host ); errorData.remote_host = NULL;
		free(errorData.databasename ); errorData.databasename = NULL;
		free(errorData.username ); errorData.username = NULL;
	}
}

/* --------------------------------
 *		pipe protocol handling
 * --------------------------------
 */

/*
 * Process data received through the syslogger pipe.
 *
 * This routine interprets the log pipe protocol which sends log messages as
 * (hopefully atomic) chunks - such chunks are detected and reassembled here.
 *
 * The protocol has a header that starts with two nul bytes, then has a 16 bit
 * length, the pid of the sending process, and a flag to indicate if it is
 * the last chunk in a message. Incomplete chunks are saved until we read some
 * more, and non-final chunks are accumulated until we get the final chunk.
 *
 * All of this is to avoid 2 problems:
 * . partial messages being written to logfiles (messes rotation), and
 * . messages from different backends being interleaved (messages garbled).
 *
 * Any non-protocol messages are written out directly. These should only come
 * from non-PostgreSQL sources, however (e.g. third party libraries writing to
 * stderr).
 *
 * logbuffer is the data input buffer, and *bytes_in_logbuffer is the number
 * of bytes present.  On exit, any not-yet-eaten data is left-justified in
 * logbuffer, and *bytes_in_logbuffer is updated.
 */
static void
process_pipe_input(char *logbuffer, int *bytes_in_logbuffer)
{
	char	   *cursor = logbuffer;
	int			count = *bytes_in_logbuffer;
	int			dest = LOG_DESTINATION_STDERR;

	/* While we have enough for a header, process data... */
	while (count >= sizeof(PipeProtoHeader))
	{
		PipeProtoHeader p;
		int chunklen;

		/* Do we have a valid header? */
		memcpy(&p, cursor, PIPE_HEADER_SIZE);
		if (chunk_is_postgres_chunk(&p))
		{
			List	   *buffer_list;
			ListCell   *cell;
			save_buffer *existing_slot = NULL,
					   *free_slot = NULL;
			StringInfo	str;

			chunklen = PIPE_HEADER_SIZE + p.len;

			/* Fall out of loop if we don't have the whole chunk yet */
			if (count < chunklen)
				break;

			dest = (p.log_format == 'c' || p.log_format == 'f') ?
				LOG_DESTINATION_CSVLOG : LOG_DESTINATION_STDERR;

			/* Locate any existing buffer for this source pid */
			buffer_list = buffer_lists[p.pid % NBUFFER_LISTS];
			foreach(cell, buffer_list)
			{
				save_buffer *buf = (save_buffer *) lfirst(cell);

				/*
				 * Different threads in the same process may write
				 * log messages concurrently. The chunk messages
				 * should be treat differently.
				 */
				if (buf->pid == p.pid && buf->tid == p.thid)
				{
					existing_slot = buf;
					break;
				}
				if (buf->pid == 0 && free_slot == NULL)
					free_slot = buf;
			}

			if (p.is_last == 'f' || p.is_last == 'F')
			{
				/*
				 * Save a complete non-final chunk in a per-pid buffer
				 */
				if (existing_slot != NULL)
				{
					/* Add chunk to data from preceding chunks */
					str = &(existing_slot->data);
					appendBinaryStringInfo(str,
										   cursor + PIPE_HEADER_SIZE,
										   p.len);
				}
				else
				{
					/* First chunk of message, save in a new buffer */
					if (free_slot == NULL)
					{
						/*
						 * Need a free slot, but there isn't one in the list,
						 * so create a new one and extend the list with it.
						 */
						free_slot = palloc(sizeof(save_buffer));
						buffer_list = lappend(buffer_list, free_slot);
						buffer_lists[p.pid % NBUFFER_LISTS] = buffer_list;
					}
					free_slot->pid = p.pid;
					free_slot->tid = p.thid;
					str = &(free_slot->data);
					initStringInfo(str);
					appendBinaryStringInfo(str,
										   cursor + PIPE_HEADER_SIZE,
										   p.len);
				}
			}
			else
			{
				/*
				 * Final chunk --- add it to anything saved for that pid, and
				 * either way write the whole thing out.
				 */
				if (existing_slot != NULL)
				{
					str = &(existing_slot->data);
					appendBinaryStringInfo(str,
										   cursor + PIPE_HEADER_SIZE,
										   p.len);
					syslogger_log_chunk_data(&p, str->data, str->len);
					/* Mark the buffer unused, and reclaim string storage */
					existing_slot->pid = 0;
					pfree(str->data);
				}
				else
				{
					/* The whole message was one chunk, evidently. */
					if (p.is_segv_msg == 't')
					{
						/*
						 * SEGV/BUS/ILL logs should be handled separately.
						 * These messages will be at most one chunk size.
						 */
						syslogger_log_segv_chunk((PipeProtoChunk *)cursor);
					}
					else
					{
						syslogger_log_chunk_data(&p, cursor + PIPE_HEADER_SIZE, p.len);
					}
				}
			}

			/* Finished processing this chunk */
			cursor += chunklen;
			count -= chunklen;
		}
		else
		{
			/* Process non-protocol data */

			/*
			 * Look for the start of a protocol header.  If found, dump data
			 * up to there and repeat the loop.  Otherwise, dump it all and
			 * fall out of the loop.  (Note: we want to dump it all if at all
			 * possible, so as to avoid dividing non-protocol messages across
			 * logfiles.  We expect that in many scenarios, a non-protocol
			 * message will arrive all in one read(), and we want to respect
			 * the read() boundary if possible.)
			 */
			for (chunklen = 0; chunklen < count; chunklen++)
			{
				if (cursor[chunklen] == '\0')
					break;
			}

			if (chunklen >= count)
			{
				/*
				 * FIXME: I think if we didn't find '0', we should try to read some more bytes
				 * from the PIPE so that we won't divide longer non-protocol messages into
				 * two parts.
				 * An example of this looks like following:
				 * Chunk0 [protocol message 1, protocol message 2, non-protocol message...]
				 * Chunk1 [continued non-protocol message, \0, protocol message 3, ...]
				 */

				/*
				 * We didn't find a byte '0', so the whole buffer
				 * cached one 3rd party error message.
				 */
				char lastChar = cursor[chunklen - 1];
				cursor[chunklen - 1] = '\0';
				elog(LOG, "3rd party error log:\n%s%c", cursor, lastChar);
			}
			else
			{
				/* If a 3rd party error starts with bytes '0', ignore this byte. */
				if (chunklen == 0)
					chunklen++;
				else
					elog(LOG, "3rd party error log:\n%s", cursor);
			}

			/* Finished processing this chunk */
			cursor += chunklen;
			count -= chunklen;
		}
	}

	/* We don't have a full chunk, so left-align what remains in the buffer */
	if (count > 0 && cursor != logbuffer)
		memmove(logbuffer, cursor, count);
	*bytes_in_logbuffer = count;
}

/*
 * Force out any buffered data
 *
 * This is currently used only at syslogger shutdown, but could perhaps be
 * useful at other times, so it is careful to leave things in a clean state.
 */
static void
flush_pipe_input(char *logbuffer, int *bytes_in_logbuffer)
{
	int			i;

	/* Dump any incomplete protocol messages */
	for (i = 0; i < NBUFFER_LISTS; i++)
	{
		List	   *list = buffer_lists[i];
		ListCell   *cell;

		foreach(cell, list)
		{
			save_buffer *buf = (save_buffer *) lfirst(cell);

			if (buf->pid != 0)
			{
				StringInfo	str = &(buf->data);

				write_syslogger_file(str->data, str->len,
									 LOG_DESTINATION_STDERR);
				/* Mark the buffer unused, and reclaim string storage */
				buf->pid = 0;
				pfree(str->data);
			}
		}
	}

	/*
	 * Force out any remaining pipe data as-is; we don't bother trying to
	 * remove any protocol headers that may exist in it.
	 */
	if (*bytes_in_logbuffer > 0)
		write_syslogger_file(logbuffer, *bytes_in_logbuffer,
							 LOG_DESTINATION_STDERR);
	*bytes_in_logbuffer = 0;
}

static void
write_binary_to_file(const char *buffer, int count, FILE *fh)
{
	int			rc;

#ifndef WIN32
	rc = fwrite(buffer, 1, count, fh);
#else
	EnterCriticalSection(&fileSection);
	rc = fwrite(buffer, 1, count, fh);
	LeaveCriticalSection(&fileSection);
#endif

	/*
	 * Try to report any failure.  We mustn't use ereport because it would
	 * just recurse right back here, but write_stderr is OK: it will write
	 * either to the postmaster's original stderr, or to /dev/null, but never
	 * to our input pipe which would result in a different sort of looping.
	 */
	if (rc != count)
		write_stderr("could not write to log file: %s\n", strerror(errno));
}


/* --------------------------------
 *		logfile routines
 * --------------------------------
 */

/*
 * Write binary data to the currently open logfile
 *
 * On Windows the data arriving in the pipe already has CR/LF newlines,
 * so we must send it to the file without further translation.
 *
 * This is exported so that elog.c can call it when MyBackendType is B_LOGGER.
 * This allows the syslogger process to record elog messages of its own,
 * even though its stderr does not point at the syslog pipe.
 */
void write_syslogger_file_binary(const char *buffer, int count, int destination)
{
	FILE	   *logfile;

	/*
	 * If we're told to write to csvlogFile, but it's not open, dump the data
	 * to syslogFile (which is always open) instead.  This can happen if CSV
	 * output is enabled after postmaster start and we've been unable to open
	 * csvlogFile.  There are also race conditions during a parameter change
	 * whereby backends might send us CSV output before we open csvlogFile or
	 * after we close it.  Writing CSV-formatted output to the regular log
	 * file isn't great, but it beats dropping log output on the floor.
	 *
	 * Think not to improve this by trying to open csvlogFile on-the-fly.  Any
	 * failure in that would lead to recursion.
	 *
	 * The following logic is a little different from GP7.
	 * We follow the upstream pattern. Use ternary operator to set the `logfile`
	 * variable to csvlogFile or syslogFile in different situations.
	 * Thus we can write to it directly without if-else sentences.
	 * We need to ensure that everytime we call write_syslogger_file_binary, the
	 * destination should always be LOG_DESTINATION_CSVLOG
	 * or LOG_DESTINATION_STDERR.
	 */
	logfile = (destination == LOG_DESTINATION_CSVLOG &&
			   csvlogFile != NULL) ? csvlogFile : syslogFile;

	write_binary_to_file(buffer, count, logfile);
}
/*
 * Write text to the currently open logfile
 *
 * This is exported so that elog.c can call it when am_syslogger is true.
 * This allows the syslogger process to record elog messages of its own,
 * even though its stderr does not point at the syslog pipe.
 */
void write_syslogger_file(const char *buffer, int count, int destination)
{
    write_syslogger_file_binary(buffer,count, destination);
}

#ifdef WIN32

/*
 * Worker thread to transfer data from the pipe to the current logfile.
 *
 * We need this because on Windows, WaitForMultipleObjects does not work on
 * unnamed pipes: it always reports "signaled", so the blocking ReadFile won't
 * allow for SIGHUP; and select is for sockets only.
 */
static unsigned int __stdcall
pipeThread(void *arg)
{
	char		logbuffer[READ_BUF_SIZE];
	int			bytes_in_logbuffer = 0;

	for (;;)
	{
		DWORD		bytesRead;
		BOOL		result;

		result = ReadFile(syslogPipe[0],
						  logbuffer + bytes_in_logbuffer,
						  sizeof(logbuffer) - bytes_in_logbuffer,
						  &bytesRead, 0);

		/*
		 * Enter critical section before doing anything that might touch
		 * global state shared by the main thread. Anything that uses
		 * palloc()/pfree() in particular are not safe outside the critical
		 * section.
		 */
		EnterCriticalSection(&sysloggerSection);
		if (result)
		{
			DWORD		error = GetLastError();

			if (error == ERROR_HANDLE_EOF ||
				error == ERROR_BROKEN_PIPE)
				break;
			_dosmaperr(error);
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not read from logger pipe: %m")));
		}
		else if (bytesRead > 0)
		{
			bytes_in_logbuffer += bytesRead;
			process_pipe_input(logbuffer, &bytes_in_logbuffer);
		}

		/*
		 * If we've filled the current logfile, nudge the main thread to do a
		 * log rotation.
		 */
		if (Log_RotationSize > 0)
		{
			if (ftell(syslogFile) >= Log_RotationSize * 1024L ||
				(csvlogFile != NULL && ftell(csvlogFile) >= Log_RotationSize * 1024L))
				SetLatch(MyLatch);
		}
		LeaveCriticalSection(&sysloggerSection);
	}

	/* We exit the above loop only upon detecting pipe EOF */
	pipe_eof_seen = true;

	/* if there's any data left then force it out now */
	flush_pipe_input(logbuffer, &bytes_in_logbuffer);

	/* set the latch to waken the main thread, which will quit */
	SetLatch(MyLatch);

	LeaveCriticalSection(&sysloggerSection);
	_endthread();
	return 0;
}
#endif							/* WIN32 */

/*
 * Open a new logfile with proper permissions and buffering options.
 *
 * If allow_errors is true, we just log any open failure and return NULL
 * (with errno still correct for the fopen failure).
 * Otherwise, errors are treated as fatal.
 */
static FILE *
logfile_open(const char *filename, const char *mode, bool allow_errors)
{
	FILE	   *fh;
	mode_t		oumask;

	/*
	 * Note we do not let Log_file_mode disable IWUSR, since we certainly want
	 * to be able to write the files ourselves.
	 */
	oumask = umask((mode_t) ((~(Log_file_mode | S_IWUSR)) & (S_IRWXU | S_IRWXG | S_IRWXO)));
	fh = fopen(filename, mode);
	umask(oumask);

	if (fh)
	{
		setvbuf(fh, NULL, PG_IOLBF, 0);

#ifdef WIN32
		/* use CRLF line endings on Windows */
		_setmode(_fileno(fh), _O_TEXT);
#endif
	}
	else
	{
		int			save_errno = errno;

		ereport(allow_errors ? LOG : FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open log file \"%s\": %m",
						filename)));
		errno = save_errno;
	}

	return fh;
}

/*
 * perform logfile rotation.
 *
 * In GPDB, this has been modified significantly from the upstream version:
 *
 * - In PostgreSQL, one call to logfile_rotate performs rotation for both the
 *   normal and the CSV log. In GPDB, this must be called separately for both,
 *   and also for the GPDB specific 'alert' log
 * - In PostgreSQL, this resets 'rotation_requested' flag. In GPDB, the caller
 *   has to do it.
 * - In PostgreSQL, this calls set_next_rotation_time(). In GPDB, the caller
 *   has to do it once all calls to this function return true (i.e. after all
 *   rotations have been successfully completed for the current timestamp), to
 *   avoid having the filename timestamp advance multiple times per rotation.
 */
static bool
logfile_rotate(bool time_based_rotation, bool size_based_rotation,
			   const char *suffix,
               const char *log_directory, 
               const char *log_filename, 
               FILE **fh_p,
               char **last_log_file_name)
{
	char	   *filename;
	char	   *csvfilename = NULL;
	pg_time_t	fntime;
	FILE	   *fh = *fh_p;

	/*
	 * When doing a time-based rotation, invent the new logfile name based on
	 * the planned rotation time, not current time, to avoid "slippage" in the
	 * file name when we don't do the rotation immediately.
	 */
	if (time_based_rotation)
		fntime = next_rotation_time;
	else
		fntime = time(NULL);
	filename = logfile_getname(fntime, suffix, log_directory, log_filename);
	if (Log_destination & LOG_DESTINATION_CSVLOG)
		csvfilename = logfile_getname(fntime, ".csv", log_directory, log_filename);

	/*
	 * Decide whether to overwrite or append.  We can overwrite if (a)
	 * Log_truncate_on_rotation is set, (b) the rotation was triggered by
	 * elapsed time and not something else, and (c) the computed file name is
	 * different from what we were previously logging into.
	 *
	 * Note: last_file_name should never be NULL here, but if it is, append.
	 */
	if (time_based_rotation || size_based_rotation)
	{
		if (Log_truncate_on_rotation && time_based_rotation &&
			*last_log_file_name != NULL &&
			strcmp(filename, *last_log_file_name) != 0)
			fh = logfile_open(filename, "w", true);
		else
			fh = logfile_open(filename, "a", true);

		if (!fh)
		{
			/*
			 * ENFILE/EMFILE are not too surprising on a busy system; just
			 * keep using the old file till we manage to get a new one.
			 * Otherwise, assume something's wrong with Log_directory and stop
			 * trying to create files.
			 */
			if (errno != ENFILE && errno != EMFILE)
			{
				ereport(LOG,
						(errmsg("disabling automatic rotation (use SIGHUP to re-enable)")));
				rotation_disabled = true;
			}

			if (filename)
				pfree(filename);
			return false;
		}

		if (*fh_p)
			fclose(*fh_p);
		*fh_p = fh;

		/* instead of pfree'ing filename, remember it for next time */
		if ((*last_log_file_name) != NULL)
			pfree(*last_log_file_name);
		*last_log_file_name = filename;
		filename = NULL;
	}

/* 
 * In gpdb, `logfile_rotate` will be called separately for both csv and std log destination.
 * We keep the code below in order to make code merging easier.
 * Note the API for this function is different. PG upstream has size_rotation_for however gpdb
 * does not have. That's becasue we deal with size_rotation_for before calling this function.
 * We'll call this function separately for both cases and only pass the size_based_rotation
 * as arguments.
 */
#if 0
	/*
	 * Same as above, but for csv file.  Note that if LOG_DESTINATION_CSVLOG
	 * was just turned on, we might have to open csvlogFile here though it was
	 * not open before.  In such a case we'll append not overwrite (since
	 * last_csv_file_name will be NULL); that is consistent with the normal
	 * rules since it's not a time-based rotation.
	 */
	if ((Log_destination & LOG_DESTINATION_CSVLOG) &&
		(csvlogFile == NULL ||
		 time_based_rotation || (size_rotation_for & LOG_DESTINATION_CSVLOG)))
	{
		if (Log_truncate_on_rotation && time_based_rotation &&
			last_csv_file_name != NULL &&
			strcmp(csvfilename, last_csv_file_name) != 0)
			fh = logfile_open(csvfilename, "w", true);
		else
			fh = logfile_open(csvfilename, "a", true);

		if (!fh)
		{
			/*
			 * ENFILE/EMFILE are not too surprising on a busy system; just
			 * keep using the old file till we manage to get a new one.
			 * Otherwise, assume something's wrong with Log_directory and stop
			 * trying to create files.
			 */
			if (errno != ENFILE && errno != EMFILE)
			{
				ereport(LOG,
						(errmsg("disabling automatic rotation (use SIGHUP to re-enable)")));
				rotation_disabled = true;
			}

			if (filename)
				pfree(filename);
			if (csvfilename)
				pfree(csvfilename);
			return;
		}

		if (csvlogFile != NULL)
			fclose(csvlogFile);
		csvlogFile = fh;

		/* instead of pfree'ing filename, remember it for next time */
		if (last_csv_file_name != NULL)
			pfree(last_csv_file_name);
		last_csv_file_name = csvfilename;
		csvfilename = NULL;
	}
	else if (!(Log_destination & LOG_DESTINATION_CSVLOG) &&
			 csvlogFile != NULL)
	{
		/* CSVLOG was just turned off, so close the old file */
		fclose(csvlogFile);
		csvlogFile = NULL;
		if (last_csv_file_name != NULL)
			pfree(last_csv_file_name);
		last_csv_file_name = NULL;
	}
#endif

	if (filename)
		pfree(filename);

	return true;
}


/*
 * construct logfile name using timestamp information
 *
 * In Postgres, if suffix isn't NULL, append it to the name, replacing any ".log"
 * that may be in the pattern.
 *
 * In GPDB, parameter suffix is not used. A separate refactor is needed for the API change.
 *
 * Result is palloc'd.
 */
static char *
logfile_getname(pg_time_t timestamp, const char *suffix,
				const char *log_directory, const char *log_file_pattern)
{
	char	   *filename;
	int			len;
	char	   *tmp_suffix;
#define CSV_SUFFIX ".csv"
#define LOG_SUFFIX ".log"

	filename = palloc(MAXPGPATH);

	snprintf(filename, MAXPGPATH, "%s/", log_directory);

	len = strlen(filename);

	/* treat Log_filename as a strftime pattern */
	pg_strftime(filename + len, MAXPGPATH - len, log_file_pattern,
				pg_localtime(&timestamp, log_timezone));

	/*
	 * If the logging format is 'TEXT' and the filename ends with ".csv",
	 * replace ".csv" with ".log".
	 *
	 * If the logging format is 'CSV' and the filename does not end with ".csv",
	 * replace the last four characters in the filename with ".csv".
	 */
	if (strlen(filename) - sizeof(CSV_SUFFIX) + 1 > 0)
	{
		tmp_suffix = filename + (strlen(filename) - sizeof(CSV_SUFFIX) + 1);
	}
	else
	{
		/*
		 * Point the tmp_suffix to the end of string if the length of
		 * the filename is less than ".csv".
		 */
		tmp_suffix = filename + strlen(filename);
	}

	/*
	 * Only change .csv to .log if gp_log_format is TEXT, otherwise leave it.
	 */
	if (gp_log_format == 0 && pg_strcasecmp(tmp_suffix, CSV_SUFFIX) == 0)
	{
		snprintf(tmp_suffix, sizeof(LOG_SUFFIX), LOG_SUFFIX);
	}

	if (gp_log_format == 1 && pg_strcasecmp(tmp_suffix, CSV_SUFFIX) != 0)
	{
		snprintf(tmp_suffix, sizeof(CSV_SUFFIX), CSV_SUFFIX);
	}

	return filename;
}

/*
 * Determine the next planned rotation time, and store in next_rotation_time.
 */
static void
set_next_rotation_time(void)
{
	pg_time_t	now;
	struct pg_tm *tm;
	int			rotinterval;

	/* nothing to do if time-based rotation is disabled */
	if (Log_RotationAge <= 0)
		return;

	/*
	 * The requirements here are to choose the next time > now that is a
	 * "multiple" of the log rotation interval.  "Multiple" can be interpreted
	 * fairly loosely.  In this version we align to log_timezone rather than
	 * GMT.
	 */
	rotinterval = Log_RotationAge * SECS_PER_MINUTE;	/* convert to seconds */
	now = (pg_time_t) time(NULL);
	tm = pg_localtime(&now, log_timezone);
	now += tm->tm_gmtoff;
	now -= now % rotinterval;
	now += rotinterval;
	now -= tm->tm_gmtoff;
	next_rotation_time = now;
}

/*
 * Store the name of the file(s) where the log collector, when enabled, writes
 * log messages.  Useful for finding the name(s) of the current log file(s)
 * when there is time-based logfile rotation.  Filenames are stored in a
 * temporary file and which is renamed into the final destination for
 * atomicity.  The file is opened with the same permissions as what gets
 * created in the data directory and has proper buffering options.
 */
static void
update_metainfo_datafile(void)
{
	FILE	   *fh;
	mode_t		oumask;

	if (!(Log_destination & LOG_DESTINATION_STDERR) &&
		!(Log_destination & LOG_DESTINATION_CSVLOG))
	{
		if (unlink(LOG_METAINFO_DATAFILE) < 0 && errno != ENOENT)
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not remove file \"%s\": %m",
							LOG_METAINFO_DATAFILE)));
		return;
	}

	/* use the same permissions as the data directory for the new file */
	oumask = umask(pg_mode_mask);
	fh = fopen(LOG_METAINFO_DATAFILE_TMP, "w");
	umask(oumask);

	if (fh)
	{
		setvbuf(fh, NULL, PG_IOLBF, 0);

#ifdef WIN32
		/* use CRLF line endings on Windows */
		_setmode(_fileno(fh), _O_TEXT);
#endif
	}
	else
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						LOG_METAINFO_DATAFILE_TMP)));
		return;
	}

	if (last_file_name && (Log_destination & LOG_DESTINATION_STDERR))
	{
		if (fprintf(fh, "stderr %s\n", last_file_name) < 0)
		{
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m",
							LOG_METAINFO_DATAFILE_TMP)));
			fclose(fh);
			return;
		}
	}

	if (last_csv_file_name && (Log_destination & LOG_DESTINATION_CSVLOG))
	{
		if (fprintf(fh, "csvlog %s\n", last_csv_file_name) < 0)
		{
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m",
							LOG_METAINFO_DATAFILE_TMP)));
			fclose(fh);
			return;
		}
	}
	fclose(fh);

	if (rename(LOG_METAINFO_DATAFILE_TMP, LOG_METAINFO_DATAFILE) != 0)
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename file \"%s\" to \"%s\": %m",
						LOG_METAINFO_DATAFILE_TMP, LOG_METAINFO_DATAFILE)));
}

/* --------------------------------
 *		signal handler routines
 * --------------------------------
 */

/*
 * Check to see if a log rotation request has arrived.  Should be
 * called by postmaster after receiving SIGUSR1.
 */
bool
CheckLogrotateSignal(void)
{
	struct stat stat_buf;

	if (stat(LOGROTATE_SIGNAL_FILE, &stat_buf) == 0)
		return true;

	return false;
}

/*
 * Remove the file signaling a log rotation request.
 */
void
RemoveLogrotateSignalFiles(void)
{
	unlink(LOGROTATE_SIGNAL_FILE);
}

/* SIGUSR1: set flag to rotate logfile */
static void
sigUsr1Handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	rotation_requested = true;
	SetLatch(MyLatch);

	errno = save_errno;
}
