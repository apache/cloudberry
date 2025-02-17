/*-------------------------------------------------------------------------
 *
 * archive.c
 *	  Common WAL archive routines
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/archive.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/archive.h"
#include "lib/stringinfo.h"

#include "cdb/cdbvars.h"

extern void pg_ltoa(int32 value, char *a);

/*
 * BuildRestoreCommand
 *
 * Builds a restore command to retrieve a file from WAL archives, replacing
 * the supported aliases with values supplied by the caller as defined by
 * the GUC parameter restore_command: xlogpath for %p, xlogfname for %f and
 * lastRestartPointFname for %r.
 *
 * The result is a palloc'd string for the restore command built.  The
 * caller is responsible for freeing it.  If any of the required arguments
 * is NULL and that the corresponding alias is found in the command given
 * by the caller, then NULL is returned.
 */
char *
BuildRestoreCommand(const char *restoreCommand,
					const char *xlogpath,
					const char *xlogfname,
					const char *lastRestartPointFname)
{
	StringInfoData result;
	const char *sp;
#ifndef FRONTEND
	char        contentid[12];  /* sign, 10 digits and '\0' */
#endif

	/*
	 * Build the command to be executed.
	 */
	initStringInfo(&result);

	for (sp = restoreCommand; *sp; sp++)
	{
		if (*sp == '%')
		{
			switch (sp[1])
			{
				case 'p':
					{
						char	   *nativePath;

						/* %p: relative path of target file */
						if (xlogpath == NULL)
						{
							pfree(result.data);
							return NULL;
						}
						sp++;

						/*
						 * This needs to use a placeholder to not modify the
						 * input with the conversion done via
						 * make_native_path().
						 */
						nativePath = pstrdup(xlogpath);
						make_native_path(nativePath);
						appendStringInfoString(&result,
											   nativePath);
						pfree(nativePath);
						break;
					}
				case 'f':
					/* %f: filename of desired file */
					if (xlogfname == NULL)
					{
						pfree(result.data);
						return NULL;
					}
					sp++;
					appendStringInfoString(&result, xlogfname);
					break;
				case 'r':
					/* %r: filename of last restartpoint */
					if (lastRestartPointFname == NULL)
					{
						pfree(result.data);
						return NULL;
					}
					sp++;
					appendStringInfoString(&result,
										   lastRestartPointFname);
					break;
#ifndef FRONTEND
				/* GPDB_13_MERGE_FIXME: How to set GpIdentity for frontend?
				 * Discussion: https://postgr.es/m/a3acff50-5a0d-9a2c-b3b2-ee36168955c1@postgrespro.ru
				 * */
				case 'c':
					/* GPDB: %c: contentId of segment */
					Assert(GpIdentity.segindex != UNINITIALIZED_GP_IDENTITY_VALUE);
					sp++;
					pg_ltoa(GpIdentity.segindex, contentid);
					appendStringInfoString(&result, contentid);
					break;
#endif
				case '%':
					/* convert %% to a single % */
					sp++;
					appendStringInfoChar(&result, *sp);
					break;
				default:
					/* otherwise treat the % as not special */
					appendStringInfoChar(&result, *sp);
					break;
			}
		}
		else
		{
			appendStringInfoChar(&result, *sp);
		}
	}

	return result.data;
}
