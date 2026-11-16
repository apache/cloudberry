/*-------------------------------------------------------------------------
 *
 * libpq-be-fe-helpers.h
 *	  Helper functions for using libpq in extensions
 *
 * Code built directly into the backend is not allowed to link to libpq
 * directly. Extension code is allowed to use libpq however. However, libpq
 * used in extensions has to be careful to block inside libpq, otherwise
 * interrupts will not be processed, leading to issues like unresolvable
 * deadlocks. Backend code also needs to take care to acquire/release an
 * external fd for the connection, otherwise fd.c's accounting of fd's is
 * broken.
 *
 * This file provides helper functions to make it easier to comply with these
 * rules. It is a header only library as it needs to be linked into each
 * extension using libpq, and it seems too small to be worth adding a
 * dedicated static library for.
 *
 * TODO: For historical reasons the connections established here are not put
 * into non-blocking mode. That can lead to blocking even when only the async
 * libpq functions are used. This should be fixed.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/libpq-be-fe-helpers.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LIBPQ_BE_FE_HELPERS_H
#define LIBPQ_BE_FE_HELPERS_H

/*
 * Despite the name, BUILDING_DLL is set only when building code directly part
 * of the backend. Which also is where libpq isn't allowed to be
 * used. Obviously this doesn't protect against libpq-fe.h getting included
 * otherwise, but perhaps still protects against a few mistakes...
 */
#ifdef BUILDING_DLL
#error "libpq may not be used code directly built into the backend"
#endif

#include "libpq-fe.h"
#include "miscadmin.h"


extern bool libpqsrv_is_valid_proxy_option(const char *keyword);

/*
 * Ensures that the provided conninfo options are considered safe for use by
 * server-side proxies (FDWs, subscriptions, et al).
 *
 * Server extensions should call this function, which will ereport(ERROR) if an
 * unblessed libpq parameter has been passed, before calling libpqsrv_connect or
 * its variants. This keeps new options from later libpq versions from breaking
 * security assumptions for existing releases during a platform library update.
 *
 * Note that the list of blessed options is subject to change during minor
 * version updates within a major release line.
 */
static inline void
libpqsrv_check_proxy_conninfo_opts(const PQconninfoOption *opts)
{
	const PQconninfoOption *opt;

	for (opt = opts; opt->keyword != NULL; opt++)
	{
		if (opt->val != NULL && !libpqsrv_is_valid_proxy_option(opt->keyword))
			ereport(ERROR,
					errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					errmsg("permission denied to set connection option \"%s\"",
						   opt->keyword));
	}
}

/*
 * Like libpqsrv_check_proxy_conninfo_opts() above, but additionally parses the
 * conninfo string for the caller.
 */
static inline void
libpqsrv_check_proxy_conninfo(const char *conninfo)
{
	PQconninfoOption *options;
	char	   *error;

	options = PQconninfoParse(conninfo, &error);

	PG_TRY();
	{
		if (!options)
		{
			/*
			 * Most likely an invalid connection string, but it could also be
			 * memory problems. The message and detail here are chosen to
			 * mimic the historical behavior of dblink.
			 */
			ereport(ERROR,
					error
					? errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION)
					: errcode(ERRCODE_OUT_OF_MEMORY),
					errmsg("could not establish connection"),
					error
					? errdetail_internal("%s", pchomp(error))
					: errdetail("out of memory"));
		}

		libpqsrv_check_proxy_conninfo_opts(options);
	}
	PG_FINALLY();
	{
		if (error)
			PQfreemem(error);
		if (options)
			PQconninfoFree(options);
	}
	PG_END_TRY();
}


#endif /* LIBPQ_BE_FE_HELPERS_H */
