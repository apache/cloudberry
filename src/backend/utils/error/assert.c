/*-------------------------------------------------------------------------
 *
 * assert.c
 *	  Assert support code.
 *
 * Portions Copyright (c) 2005-2009, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/error/assert.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq/pqsignal.h"
#include "cdb/cdbvars.h"                /* gp_reraise_signal */

#include <unistd.h>
#ifdef HAVE_EXECINFO_H
#include <execinfo.h>
#endif

/*
 * ExceptionalCondition - Handles the failure of an Assert()
 *
 * We intentionally do not go through elog() here, on the grounds of
 * wanting to minimize the amount of infrastructure that has to be
 * working to report an assertion failure.
 */
void
ExceptionalCondition(const char *conditionName,
					 const char *fileName,
					 int lineNumber)
{
    /* CDB: Try to tell the QD or client what happened. */
	if (!PointerIsValid(conditionName)
		|| !PointerIsValid(fileName))
		ereport(FATAL,
				errFatalReturn(gp_reraise_signal),
				errmsg("TRAP: ExceptionalCondition: bad arguments"));
	else
		ereport(FATAL,
				errFatalReturn(gp_reraise_signal),
				errmsg("Unexpected internal error"),
				errdetail("(\"%s\", File: \"%s\", Line: %d)\n",
						  conditionName, fileName, lineNumber));

	/* Usually this shouldn't be needed, but make sure the msg went out */
	fflush(stderr);

	/* If we have support for it, dump a simple backtrace */
#ifdef HAVE_BACKTRACE_SYMBOLS
	{
		void	   *buf[100];
		int			nframes;

		nframes = backtrace(buf, lengthof(buf));
		backtrace_symbols_fd(buf, nframes, fileno(stderr));
	}
#endif

	/*
	 * If configured to do so, sleep indefinitely to allow user to attach a
	 * debugger.  It would be nice to use pg_usleep() here, but that can sleep
	 * at most 2G usec or ~33 minutes, which seems too short.
	 */
#ifdef SLEEP_ON_ASSERT
	sleep(1000000);
#endif

	abort();
}
