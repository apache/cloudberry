/*-------------------------------------------------------------------------
 *
 * cdbutil.h
 *	  Header file for routines in cdbutil.c and results returned by
 *	  those routines.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbutil.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBUTIL_H
#define CDBUTIL_H
#include "postgres.h"
#ifdef USE_INTERNAL_FTS
#include "catalog/gp_segment_configuration.h"
#include "catalog/gp_segment_configuration_indexing.h"
#endif
#include "postmaster/fts_comm.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "utils/palloc.h"
#include <sys/param.h>			/* for MAXHOSTNAMELEN */

struct SegmentDatabaseDescriptor;

extern MemoryContext CdbComponentsContext;

extern bool gp_etcd_enable_cache;
extern char *gp_etcd_account_id;
extern char *gp_etcd_cluster_id;
extern char *gp_etcd_namespace;
extern char *gp_etcd_endpoints;

extern char *gp_segment_configuration_file;

typedef struct GpSegConfigEntryForUDF
{
	GpSegConfigEntry * config_entry;
	int16 current_index;
	int16 total_dbs;
} GpSegConfigEntryForUDF;

typedef enum SegmentType
{
	SEGMENTTYPE_EXPLICT_WRITER = 1,
	SEGMENTTYPE_EXPLICT_READER,
	SEGMENTTYPE_ANY
}SegmentType;

/*
 * performs all necessary setup required for initializing Apache Cloudberry components.
 *
 * This includes cdblink_setup() and initializing the Motion Layer.
 *
 */
extern void cdb_setup(void);


/*
 * performs all necessary cleanup required when cleaning up Apache Cloudberry components
 * when disabling Apache Cloudberry functionality.
 *
 */
extern void cdb_cleanup(int code, Datum arg  pg_attribute_unused() );


/*
 * cdbcomponent_getCdbComponents() returns a pointer to a CdbComponentDatabases
 * structure. Both the segment_db_info array and the entry_db_info_array are ordered by segindex,
 * isprimary desc.
 *
 * The CdbComponentDatabases structure, and the segment_db_info array and the entry_db_info_array
 * are contained in palloc'd storage allocated from the current storage context.
 * The same is true for pointer-based values in CdbComponentDatabaseInfo.  The caller is responsible
 * for setting the current storage context and releasing the storage occupied the returned values.
 */
CdbComponentDatabases * cdbcomponent_getCdbComponents(void);
void cdbcomponent_destroyCdbComponents(void);

void cdbcomponent_updateCdbComponents(void);
/*
 * cdbcomponent_cleanupIdleQEs()
 *
 * This routine is used when a session has been idle for a while (waiting for the
 * client to send us SQL to execute). The idea is to consume less resources while sitting idle.
 *
 * The expectation is that if the session is logged on, but nobody is sending us work to do,
 * we want to free up whatever resources we can. Usually it means there is a human being at the
 * other end of the connection, and that person has walked away from their terminal, or just hasn't
 * decided what to do next. We could be idle for a very long time (many hours).
 *
 * Of course, freeing QEs means that the next time the user does send in an SQL statement,
 * we need to allocate QEs (at least the writer QEs) to do anything. This entails extra work,
 * so we don't want to do this if we don't think the session has gone idle.
 *
 * Only call these routines from an idle session.
 *
 * This routine is also called from the sigalarm signal handler (hopefully that is safe to do).
 */
void cdbcomponent_cleanupIdleQEs(bool includeWriter);

CdbComponentDatabaseInfo * cdbcomponent_getComponentInfo(int contentId);

struct SegmentDatabaseDescriptor * cdbcomponent_allocateIdleQE(int contentId, SegmentType segmentType);

void cdbcomponent_recycleIdleQE(struct SegmentDatabaseDescriptor *segdbDesc, bool forceDestroy);

bool cdbcomponent_qesExist(void);
bool cdbcomponent_activeQEsExist(void);

List *cdbcomponent_getCdbComponentsList(void);

/*
 * Given total number of primary segment databases and a number of segments
 * to "skip" - this routine creates a boolean map (array) the size of total
 * number of segments and randomly selects several entries (total number of
 * total_to_skip) to be marked as "skipped". This is used for external tables
 * with the 'gpfdist' protocol where we want to get a number of *random* segdbs
 * to connect to a gpfdist client.
 */
extern bool *makeRandomSegMap(int total_primaries, int total_to_skip);

/*
 * do a host:port to IP lookup, bypass caching.
 */
extern char *getDnsAddress(char *name, int port, int elevel);


#ifdef USE_INTERNAL_FTS
extern void writeGpSegConfigToFTSFiles(void);
extern void add_segment_config_entry(GpSegConfigEntry *i);
extern void remove_segment_config_entry(int16 dbid);
extern void write_gp_segment_configuration(void);
#else

GpSegConfigEntry * readGpSegConfig(char * buff, int *total_dbs);
GpSegConfigEntry * readGpSegConfigFromETCDAllowNull(int *total_dbs);
GpSegConfigEntry * readGpSegConfigFromETCD(int *total_dbs, bool allow_null);
void cleanGpSegConfigs(GpSegConfigEntry *configs, int total_dbs);

bool setStandbyPromoteReady(bool standby_promote_ready);
bool getStandbyPromoteReady(bool *standby_promote_ready);

/* segment configurations cache ops */ 
Size ShmemSegmentConfigsCacheSize(void);
void ShmemSegmentConfigsCacheAllocation(void);
void ShmemSegmentConfigsCacheReset(void);
void ShmemSegmentConfigsCacheForceFlush(void);

bool isSegmentConfigsCacheEnable(void);
bool isSegmentConfigsCached(void);
char *readSegmentConfigsCache(int *total_dbs);
void writeSegmentConfigsCacheBuff(char * config_buff, int buff_length, int total_dbs);


/* segment configurations ops */ 
void addSegment(GpSegConfigEntry *new_segment_information);
void delSegment(int16 dbid);
void rewriteSegments(char *rewrite_string, bool allow_diff_db_counts);
void cleanSegments(void);
void activateStandby(int16 standby_dbid, int16 master_dbid);
void updateSegmentModeStatus(int16 dbid, char mode, char status);

#endif

extern int16 master_standby_dbid(void);
extern GpSegConfigEntry *dbid_get_dbinfo(int16 dbid);
extern int16 contentid_get_dbid(int16 contentid, char role, bool getPreferredRoleNotCurrentRole);
extern int16 cdbcomponent_get_maxdbid(void);
extern int16 cdbcomponent_get_availableDbId(void);
extern int16 cdbcomponent_get_maxcontentid(void);

extern int numsegmentsFromQD;
/*
 * Returns the number of segments
 */
extern int getgpsegmentCount(void);

extern bool IsOnConflictUpdate(PlannedStmt *ps);

extern void AvoidCorefileGeneration(void);

#define ELOG_DISPATCHER_DEBUG(...) do { \
       if (gp_log_gang >= GPVARS_VERBOSITY_DEBUG) elog(LOG, __VA_ARGS__); \
    } while(false);



#endif   /* CDBUTIL_H */
