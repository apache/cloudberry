/*-------------------------------------------------------------------------
 *
 * copydir.h
 *	  Copy a directory.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/copydir.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPYDIR_H
#define COPYDIR_H

extern void copydir(char *fromdir, char *todir, bool recurse);
extern void copy_file(char *fromfile, char *tofile, bool encrypt_init_file);

#endif							/* COPYDIR_H */
