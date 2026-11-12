/*-------------------------------------------------------------------------
 *
 * twophase_xlog.h
 *	  Two-phase-commit state file.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/twophase_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TWOPHASE_XLOG_H
#define TWOPHASE_XLOG_H

#include "access/xact.h"

/*
 * Header for a 2PC state file.
 *
 * Upstream PostgreSQL defines this typedef privately in twophase.c; GPDB
 * exposes it here because the WAL descriptors need it too.  It is the same
 * structure as the header of a XLOG_XACT_PREPARE record, so it is a typedef
 * of xl_xact_prepare rather than a second copy of the field list -- keeping
 * two copies in sync by hand is how the layouts drifted apart before.
 */
typedef xl_xact_prepare TwoPhaseFileHeader;

#endif							/* TWOPHASE_XLOG_H */
