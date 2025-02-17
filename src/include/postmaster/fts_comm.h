/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * fts_comm.h
 *	  Common interface for fault tolerance service (FTS).
 *    Also used in src/bin/gpfts
 *    Don't define any method in current header
 * 
 * IDENTIFICATION
 *		src/include/postmaster/fts_comm.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef FTS_COMM_H
#define FTS_COMM_H

#include "nodes/pg_list.h"


#define FTS_DUMP_FILE_KEY "fts_dump_file_key"
#define FTS_STANDBY_PROMOTE_READY_KEY "fts_standby_promote_ready_key"
#define FTS_STANDBY_PROMOTE_READY "1"
#define FTS_STANDBY_PROMOTE_NO_READY "0"
#define FTS_STANDBY_PROMOTE_LEN 1

/* Queries for FTS messages */
#define	FTS_MSG_PROBE "PROBE"
#define FTS_MSG_SYNCREP_OFF "SYNCREP_OFF"
#define FTS_MSG_PROMOTE "PROMOTE"

/*
 * This is used for constructing string to store the full fts message request
 * string from QD to QE. Format for which is defined using FTS_MSG_FORMAT and
 * first part of it string to define type of fts message like FTS_MSG_PROBE,
 * FTS_MSG_SYNCREP_OFF or FTS_MSG_PROMOTE.
 */
#define FTS_MSG_MAX_LEN 100

/*
 * It consists of the following field.
 * dbid| content| role| preferred_role| mode| status| port| hostname| address| datadir|
 */
#ifdef USE_INTERNAL_FTS
#define GPSEGCONFIGNUMATTR 9
#else
#define GPSEGCONFIGNUMATTR 10
#endif

/*
 * Defines for gp_segment_configuration table
 */
#define GpSegmentConfigRelationName		"gp_segment_configuration"

#define MASTER_CONTENT_ID (-1)

#define GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY 'p'
#define GP_SEGMENT_CONFIGURATION_ROLE_MIRROR 'm'

#define GP_SEGMENT_CONFIGURATION_STATUS_UP 'u'
#define GP_SEGMENT_CONFIGURATION_STATUS_DOWN 'd'

#define GP_SEGMENT_CONFIGURATION_MODE_INSYNC 's'
#define GP_SEGMENT_CONFIGURATION_MODE_NOTINSYNC 'n'

#define SEGMENT_IS_ACTIVE_MIRROR(p) \
	((p)->config->role == GP_SEGMENT_CONFIGURATION_ROLE_MIRROR ? true : false)
#define SEGMENT_IS_ACTIVE_PRIMARY(p) \
	((p)->config->role == GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY ? true : false)

#define SEGMENT_IS_ALIVE(p) \
	((p)->config->status == GP_SEGMENT_CONFIGURATION_STATUS_UP ? true : false)

#define SEGMENT_IS_IN_SYNC(p) \
	((p)->config->mode == GP_SEGMENT_CONFIGURATION_MODE_INSYNC ? true : false)
#define SEGMENT_IS_NOT_INSYNC(p) \
	((p)->config->mode == GP_SEGMENT_CONFIGURATION_MODE_NOTINSYNC ? true : false)

#define SEGMENT_CONFIGURATION_ONE_LINE_LENGTH MAXHOSTNAMELEN * 2 + 32 + 4096
#define SEGMENT_CONFIGURATION_CACHE_COUNTS 500
#define SEGMENT_CONFIGURATION_ALLOC_COUNTS SEGMENT_CONFIGURATION_CACHE_COUNTS

/*
 * If altering the fts message format, consider if FTS_MSG_MAX_LEN is enough
 * to store the modified format.
 */
#define FTS_MSG_FORMAT "%s dbid=%d contid=%d"

#define Natts_fts_message_response 5
#define Anum_fts_message_response_is_mirror_up 0
#define Anum_fts_message_response_is_in_sync 1
#define Anum_fts_message_response_is_syncrep_enabled 2
#define Anum_fts_message_response_is_role_mirror 3
#define Anum_fts_message_response_request_retry 4

#define FTS_MESSAGE_RESPONSE_NTUPLES 1


typedef struct CdbComponentDatabaseInfo CdbComponentDatabaseInfo;
typedef struct CdbComponentDatabases CdbComponentDatabases;

struct CdbComponentDatabases
{
	CdbComponentDatabaseInfo *segment_db_info;	/* array of
												 * SegmentDatabaseInfo's for
												 * segment databases */
	int			total_segment_dbs;		/* count of the array  */
	CdbComponentDatabaseInfo *entry_db_info;	/* array of
												 * SegmentDatabaseInfo's for
												 * entry databases */
	int			total_entry_dbs;	/* count of the array  */
	int			total_segments; /* count of distinct segindexes */
#ifdef USE_INTERNAL_FTS
	uint8		fts_version;	/* the version of fts */
#endif
	int			expand_version;
	int			numActiveQEs;
	int			numIdleQEs;
	int			qeCounter;
	List		*freeCounterList;
};

/* --------------------------------------------------------------------------------------------------
 * Structure for MPP 2.0 database information
 *
 * The information contained in this structure represents logically a row from
 * gp_configuration.  It is used to describe either an entry
 * database or a segment database.
 *
 * Storage for instances of this structure are palloc'd.  Storage for the values
 * pointed to by non-NULL char*'s are also palloc'd.
 *
 */
#define COMPONENT_DBS_MAX_ADDRS (8)
typedef struct GpSegConfigEntry 
{
	/* copy of entry in gp_segment_configuration */
	int16		dbid;			/* the dbid of this database */
	int16		segindex;		/* content indicator: -1 for entry database,
								 * 0, ..., n-1 for segment database */

	char		role;			/* primary, master, mirror, master-standby */
	char		preferred_role; /* what role would we "like" to have this segment in ? */
	char		mode;
	char		status;
	int32		port;			/* port that instance is listening on */
	char		*hostname;		/* name or ip address of host machine */
	char		*address;		/* ip address of host machine */
	char		*datadir;		/* absolute path to data directory on the host. */
	Oid			warehouseid;

	/* additional cached info */
	char		*hostip;	/* cached lookup of name */
	char		*hostaddrs[COMPONENT_DBS_MAX_ADDRS];	/* cached lookup of names */	
} GpSegConfigEntry;

struct CdbComponentDatabaseInfo
{
	struct GpSegConfigEntry	*config;

	CdbComponentDatabases	*cdbs; /* point to owners */

	int16		hostPrimaryCount;	/* number of primary segments on the same hosts */
	List		*freelist;	/* list of idle segment dbs */
	int			numIdleQEs;
	List		*activelist;	/* list of active segment dbs */
	int			numActiveQEs;
};

#endif