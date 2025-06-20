/*-------------------------------------------------------------------------
 *
 * cdbutil.c
 *	  Internal utility support functions for Apache Cloudberry/PostgreSQL.
 *
 * Portions Copyright (c) 2005-2011, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbutil.c
 *
 * NOTES
 *
 *	- According to src/backend/executor/execHeapScan.c
 *		"tuples returned by heap_getnext() are pointers onto disk
 *		pages and were not created with palloc() and so should not
 *		be pfree()'d"
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#include <sys/resource.h>
#endif

#include <sys/param.h>			/* for MAXHOSTNAMELEN */

#include "access/genam.h"
#include "common/ip.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "catalog/gp_id.h"
#include "catalog/indexing.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbmotion.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/ml_ipc.h"			/* listener_setup */
#include "cdb/cdbtm.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "cdb/cdbconn.h"
#include "cdb/cdbfts.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "postmaster/fts.h"
#include "postmaster/postmaster.h"
#include "catalog/namespace.h"
#include "utils/gpexpand.h"
#include "access/xact.h"
#include "common/hashfn.h"
#include "catalog/gp_indexing.h"
#include "utils/etcd.h"
#include "common/etcdutils.h"

#include "catalog/gp_indexing.h"

#define MAX_CACHED_1_GANGS 1
#define INCR_COUNT(cdbinfo, arg) \
	(cdbinfo)->arg++; \
	(cdbinfo)->cdbs->arg++;

#define DECR_COUNT(cdbinfo, arg) \
	(cdbinfo)->arg--; \
	(cdbinfo)->cdbs->arg--; \
	Assert((cdbinfo)->arg >= 0); \
	Assert((cdbinfo)->cdbs->arg >= 0); \

#define GPSEGCONFIGDUMPFILE "gpsegconfig_dump"
#define GPSEGCONFIGDUMPFILETMP "gpsegconfig_dump_tmp"

MemoryContext CdbComponentsContext = NULL;
static CdbComponentDatabases *cdb_component_dbs = NULL;

#ifdef USE_INTERNAL_FTS

/*
 * Helper Functions
 */
static CdbComponentDatabases *getCdbComponentInfo(void);
static void cleanupComponentIdleQEs(CdbComponentDatabaseInfo *cdi, bool includeWriter);

static int	CdbComponentDatabaseInfoCompare(const void *p1, const void *p2);

static GpSegConfigEntry * readGpSegConfigFromCatalog(int *total_dbs);
static GpSegConfigEntry * readGpSegConfigFromFTSFiles(int *total_dbs);

static void getAddressesForDBid(GpSegConfigEntry *c, int elevel);
static HTAB *hostPrimaryCountHashTableInit(void);

static int nextQEIdentifer(CdbComponentDatabases *cdbs);

Datum gp_get_suboverflowed_backends(PG_FUNCTION_ARGS);

static HTAB *segment_ip_cache_htab = NULL;

int numsegmentsFromQD = -1;

typedef struct SegIpEntry
{
	char		key[NAMEDATALEN];
	char		hostinfo[NI_MAXHOST];
} SegIpEntry;

typedef struct HostPrimaryCountEntry
{
	char		hostname[MAXHOSTNAMELEN];
	int			segmentCount;
} HostPrimaryCountEntry;

/*
 * Helper functions for fetching latest gp_segment_configuration outside of
 * the transaction.
 *
 * In phase 2 of 2PC, current xact has been marked to TRANS_COMMIT/ABORT,
 * COMMIT_PREPARED or ABORT_PREPARED DTM are performed, if they failed,
 * dispather disconnect and destroy all gangs and fetch the latest segment
 * configurations to do RETRY_COMMIT_PREPARED or RETRY_ABORT_PREPARED,
 * however, postgres disallow catalog lookups outside of xacts.
 *
 * readGpSegConfigFromFTSFiles() notify FTS to dump the configs from catalog
 * to a flat file and then read configurations from that file.
 */
static GpSegConfigEntry *
readGpSegConfigFromFTSFiles(int *total_dbs)
{
	FILE	*fd;
	int		idx = 0;
	int		array_size = 500;
	GpSegConfigEntry *configs = NULL;
	GpSegConfigEntry *config = NULL;

	char	hostname[MAXHOSTNAMELEN];
	char	address[MAXHOSTNAMELEN];
	char	buf[MAXHOSTNAMELEN * 2 + 32];

	Assert(!IsTransactionState());

	/* notify and wait FTS to finish a probe and update the dump file */
	FtsNotifyProber();	

	fd = AllocateFile(GPSEGCONFIGDUMPFILE, "r");

	if (!fd)
		elog(ERROR, "could not open gp_segment_configutation dump file:%s:%m", GPSEGCONFIGDUMPFILE);

	configs = palloc0(sizeof (GpSegConfigEntry) * array_size);

	while (fgets(buf, sizeof(buf), fd))
	{
		config = &configs[idx];

		if (sscanf(buf, "%d %d %c %c %c %c %d %s %s", (int *)&config->dbid, (int *)&config->segindex,
				   &config->role, &config->preferred_role, &config->mode, &config->status,
				   &config->port, hostname, address) != GPSEGCONFIGNUMATTR)
		{
			FreeFile(fd);
			elog(ERROR, "invalid data in gp_segment_configuration dump file: %s:%m", GPSEGCONFIGDUMPFILE);
		}

		config->hostname = pstrdup(hostname);
		config->address = pstrdup(address);

		idx++;
		/*
		 * Expand CdbComponentDatabaseInfo array if we've used up
		 * currently allocated space
		 */
		if (idx >= array_size)
		{
			array_size = array_size * 2;
			configs = (GpSegConfigEntry *)
				repalloc(configs, sizeof(GpSegConfigEntry) * array_size);
		}
	}

	FreeFile(fd);

	*total_dbs = idx;
	return configs;
}

/*
 * writeGpSegConfigToFTSFiles() dump gp_segment_configuration to the file
 * GPSEGCONFIGDUMPFILE, in $PGDATA, only FTS process can use this function.
 *
 * write contents to GPSEGCONFIGDUMPFILETMP first, then rename it to
 * GPSEGCONFIGDUMPFILE, it makes lockless read and write concurrently.
 */
void
writeGpSegConfigToFTSFiles(void)
{
	FILE	*fd;
	int		idx = 0;
	int		total_dbs = 0;
	GpSegConfigEntry *configs = NULL;
	GpSegConfigEntry *config = NULL;

	Assert(IsTransactionState());
	Assert(am_ftsprobe);

	fd = AllocateFile(GPSEGCONFIGDUMPFILETMP, "w+");

	if (!fd)
		elog(ERROR, "could not create tmp file: %s: %m", GPSEGCONFIGDUMPFILETMP);

	configs = readGpSegConfigFromCatalog(&total_dbs);

	for (idx = 0; idx < total_dbs; idx++)
	{
		config = &configs[idx];

		if (fprintf(fd, "%d %d %c %c %c %c %d %s %s\n", config->dbid, config->segindex,
					config->role, config->preferred_role, config->mode, config->status,
					config->port, config->hostname, config->address) < 0)
		{
			FreeFile(fd);
			elog(ERROR, "could not dump gp_segment_configuration to file: %s: %m", GPSEGCONFIGDUMPFILE);
		}
	}

	FreeFile(fd);

	/* rename tmp file to permanent file */
	if (rename(GPSEGCONFIGDUMPFILETMP, GPSEGCONFIGDUMPFILE) != 0)
		elog(ERROR, "could not rename file %s to file %s: %m",
			 GPSEGCONFIGDUMPFILETMP, GPSEGCONFIGDUMPFILE);
}

static GpSegConfigEntry *
readGpSegConfigFromCatalog(int *total_dbs)
{
	int					idx = 0;
	int					array_size;
	bool				isNull;
	Datum				attr;
	Oid					warehouseid = InvalidOid;
	Relation			gp_seg_config_rel;
	HeapTuple			gp_seg_config_tuple = NULL;
	SysScanDesc			gp_seg_config_scan;
	GpSegConfigEntry	*configs;
	GpSegConfigEntry	*config;

	array_size = 500;
	configs = palloc0(sizeof(GpSegConfigEntry) * array_size);

	gp_seg_config_rel = table_open(GpSegmentConfigRelationId, AccessShareLock);
	gp_seg_config_scan = systable_beginscan(gp_seg_config_rel, InvalidOid, false, NULL,
											0, NULL);

	while (HeapTupleIsValid(gp_seg_config_tuple = systable_getnext(gp_seg_config_scan)))
	{
		/* warehouseid */
		attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_warehouseid, RelationGetDescr(gp_seg_config_rel), &isNull);
		Assert(!isNull);
		warehouseid = DatumGetObjectId(attr);

		/* content */
		attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_content, RelationGetDescr(gp_seg_config_rel), &isNull);
		Assert(!isNull);

		if (warehouseid == GetCurrentWarehouseId() || DatumGetInt16(attr) == MASTER_CONTENT_ID)
		{
			config = &configs[idx];

			/* dbid */
			attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_dbid, RelationGetDescr(gp_seg_config_rel), &isNull);
			Assert(!isNull);
			config->dbid = DatumGetInt16(attr);

			/* content */
			attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_content, RelationGetDescr(gp_seg_config_rel), &isNull);
			Assert(!isNull);
			config->segindex= DatumGetInt16(attr);

			/* role */
			attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_role, RelationGetDescr(gp_seg_config_rel), &isNull);
			Assert(!isNull);
			config->role = DatumGetChar(attr);

			/* preferred-role */
			attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_preferred_role, RelationGetDescr(gp_seg_config_rel), &isNull);
			Assert(!isNull);
			config->preferred_role = DatumGetChar(attr);

			/* mode */
			attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_mode, RelationGetDescr(gp_seg_config_rel), &isNull);
			Assert(!isNull);
			config->mode = DatumGetChar(attr);

			/* status */
			attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_status, RelationGetDescr(gp_seg_config_rel), &isNull);
			Assert(!isNull);
			config->status = DatumGetChar(attr);

			/* hostname */
			attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_hostname, RelationGetDescr(gp_seg_config_rel), &isNull);
			Assert(!isNull);
			config->hostname = TextDatumGetCString(attr);

			/* address */
			attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_address, RelationGetDescr(gp_seg_config_rel), &isNull);
			Assert(!isNull);
			config->address = TextDatumGetCString(attr);

			/* port */
			attr = heap_getattr(gp_seg_config_tuple, Anum_gp_segment_configuration_port, RelationGetDescr(gp_seg_config_rel), &isNull);
			Assert(!isNull);
			config->port = DatumGetInt32(attr);

			/* datadir is not dumped*/

			idx++;

			/*
			* Expand CdbComponentDatabaseInfo array if we've used up
			* currently allocated space
			*/
			if (idx >= array_size)
			{
				array_size = array_size * 2;
				configs = (GpSegConfigEntry *)
					repalloc(configs, sizeof(GpSegConfigEntry) * array_size);
			}
		}
	}

	/*
	 * We're done with the catalog config, clean them up, closing all the
	 * relations we opened.
	 */
	systable_endscan(gp_seg_config_scan);
	table_close(gp_seg_config_rel, AccessShareLock);

	*total_dbs = idx;
	return configs;
}

/*
 *  Internal function to initialize each component info
 */
static CdbComponentDatabases *
getCdbComponentInfo(void)
{
	MemoryContext oldContext;
	CdbComponentDatabaseInfo *cdbInfo;
	CdbComponentDatabases *component_databases = NULL;
	GpSegConfigEntry *configs;

	int			i;
	int			x = 0;
	int			total_dbs = 0;

	bool		found;
	HostPrimaryCountEntry *hsEntry;

	if (!CdbComponentsContext)
		CdbComponentsContext = AllocSetContextCreate(TopMemoryContext, "cdb components Context",
								ALLOCSET_DEFAULT_MINSIZE,
								ALLOCSET_DEFAULT_INITSIZE,
								ALLOCSET_DEFAULT_MAXSIZE);

	oldContext = MemoryContextSwitchTo(CdbComponentsContext);

	HTAB	   *hostPrimaryCountHash = hostPrimaryCountHashTableInit();

	if (IsTransactionState())
		configs = readGpSegConfigFromCatalog(&total_dbs);
	else
		configs = readGpSegConfigFromFTSFiles(&total_dbs);

	component_databases = palloc0(sizeof(CdbComponentDatabases));

	component_databases->numActiveQEs = 0;
	component_databases->numIdleQEs = 0;
	component_databases->qeCounter = 0;
	component_databases->freeCounterList = NIL;

	component_databases->segment_db_info =
		(CdbComponentDatabaseInfo *) palloc0(sizeof(CdbComponentDatabaseInfo) * total_dbs);

	component_databases->entry_db_info =
		(CdbComponentDatabaseInfo *) palloc0(sizeof(CdbComponentDatabaseInfo) * 2);

	for (i = 0; i < total_dbs; i++)
	{
		CdbComponentDatabaseInfo	*pRow;
		GpSegConfigEntry	*config = &configs[i];

		if (config->hostname == NULL || strlen(config->hostname) > MAXHOSTNAMELEN)
		{
			/*
			 * We should never reach here, but add sanity check
			 * The reason we check length is we find MAXHOSTNAMELEN might be
			 * smaller than the ones defined in /etc/hosts. Those are rare cases.
			 */
			elog(ERROR,
				 "Invalid length (%d) of hostname (%s)",
				 config->hostname == NULL ? 0 : (int) strlen(config->hostname),
				 config->hostname == NULL ? "" : config->hostname);
		}

		/* lookup hostip/hostaddrs cache */
		config->hostip= NULL;
		getAddressesForDBid(config, !am_ftsprobe? ERROR : LOG);

		/*
		 * We make sure we get a valid hostip for primary here,
		 * if hostip for mirrors can not be get, ignore the error.
		 */
		if (config->hostaddrs[0] == NULL &&
			config->role == GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY)
			ereport(!am_ftsprobe ? ERROR : LOG,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					errmsg("cannot resolve network address for dbid=%d", config->dbid)));

		if (config->hostaddrs[0] != NULL)
			config->hostip = pstrdup(config->hostaddrs[0]);
		AssertImply(config->hostip, strlen(config->hostip) <= INET6_ADDRSTRLEN);

		/*
		 * Determine which array to place this rows data in: entry or segment,
		 * based on the content field.
		 */
		if (config->segindex >= 0)
		{
			pRow = &component_databases->segment_db_info[component_databases->total_segment_dbs];
			component_databases->total_segment_dbs++;
		}
		else
		{
			pRow = &component_databases->entry_db_info[component_databases->total_entry_dbs];
			component_databases->total_entry_dbs++;
		}

		pRow->cdbs = component_databases;
		pRow->config = config;
		pRow->freelist = NIL;
		pRow->activelist = NIL;
		pRow->numIdleQEs = 0;
		pRow->numActiveQEs = 0;

		if (config->role != GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY)
			continue;

		hsEntry = (HostPrimaryCountEntry *) hash_search(hostPrimaryCountHash, config->hostname, HASH_ENTER, &found);
		if (found)
			hsEntry->segmentCount++;
		else
			hsEntry->segmentCount = 1;
	}

	/*
	 * Validate that there exists at least one entry and one segment database
	 * in the configuration
	 */
	/*
	 * In singlenode deployment, total_segment_dbs is zero and it should still work.
	 */
	if (component_databases->total_segment_dbs == 0 && !IS_SINGLENODE())
	{
		ereport(ERROR,
				(errcode(ERRCODE_CARDINALITY_VIOLATION),
				 errmsg("number of segment databases cannot be 0")));
	}
	if (component_databases->total_entry_dbs == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CARDINALITY_VIOLATION),
				 errmsg("number of entry databases cannot be 0")));
	}

	/*
	 * Now sort the data by segindex, isprimary desc
	 */
	qsort(component_databases->segment_db_info,
	      component_databases->total_segment_dbs, sizeof(CdbComponentDatabaseInfo),
	      CdbComponentDatabaseInfoCompare);

	qsort(component_databases->entry_db_info,
		  component_databases->total_entry_dbs, sizeof(CdbComponentDatabaseInfo),
		  CdbComponentDatabaseInfoCompare);

	/*
	 * Now count the number of distinct segindexes. Since it's sorted, this is
	 * easy.
	 */
	for (i = 0; i < component_databases->total_segment_dbs; i++)
	{
		if (i == 0 ||
			(component_databases->segment_db_info[i].config->segindex != component_databases->segment_db_info[i - 1].config->segindex))
		{
			component_databases->total_segments++;
		}
	}

	/*
	 * Now validate that our identity is present in the entry databases
	 */
	for (i = 0; i < component_databases->total_entry_dbs; i++)
	{
		cdbInfo = &component_databases->entry_db_info[i];

		if (cdbInfo->config->dbid == GpIdentity.dbid && cdbInfo->config->segindex == GpIdentity.segindex)
		{
			break;
		}
	}
	if (i == component_databases->total_entry_dbs)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("cannot locate entry database"),
				 errdetail("Entry database represented by this db in gp_segment_configuration: dbid %d content %d",
						   GpIdentity.dbid, GpIdentity.segindex)));
	}

	/*
	 * Now validate that the segindexes for the segment databases are between
	 * 0 and (numsegments - 1) inclusive, and that we hit them all.
	 * Since it's sorted, this is relatively easy.
	 */
	x = 0;
	for (i = 0; i < component_databases->total_segments; i++)
	{
		int			this_segindex = -1;

		while (x < component_databases->total_segment_dbs)
		{
			this_segindex = component_databases->segment_db_info[x].config->segindex;
			if (this_segindex < i)
				x++;
			else if (this_segindex == i)
				break;
			else if (this_segindex > i)
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATA_EXCEPTION),
						 errmsg("content values not valid in %s table",
								GpSegmentConfigRelationName),
						 errdetail("Content values must be in the range 0 to %d inclusive.",
								   component_databases->total_segments - 1)));
			}
		}
		if (this_segindex != i)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("content values not valid in %s table",
							GpSegmentConfigRelationName),
					 errdetail("Content values must be in the range 0 to %d inclusive",
							   component_databases->total_segments - 1)));
		}
	}

	for (i = 0; i < component_databases->total_segment_dbs; i++)
	{
		cdbInfo = &component_databases->segment_db_info[i];

		if (!IS_HOT_STANDBY_QD() && cdbInfo->config->role != GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY)
			continue;

		hsEntry = (HostPrimaryCountEntry *) hash_search(hostPrimaryCountHash, cdbInfo->config->hostname, HASH_FIND, &found);
		Assert(found);
		cdbInfo->hostPrimaryCount = hsEntry->segmentCount;
	}

	for (i = 0; i < component_databases->total_entry_dbs; i++)
	{
		cdbInfo = &component_databases->entry_db_info[i];

		if (!IS_HOT_STANDBY_QD() && cdbInfo->config->role != GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY)
			continue;

		hsEntry = (HostPrimaryCountEntry *) hash_search(hostPrimaryCountHash, cdbInfo->config->hostname, HASH_FIND, &found);
		Assert(found);
		cdbInfo->hostPrimaryCount = hsEntry->segmentCount;
	}

	hash_destroy(hostPrimaryCountHash);

	MemoryContextSwitchTo(oldContext);

	return component_databases;
}

/*
 * Helper function to clean up the idle segdbs list of
 * a segment component.
 */
static void
cleanupComponentIdleQEs(CdbComponentDatabaseInfo *cdi, bool includeWriter)
{
	SegmentDatabaseDescriptor	*segdbDesc;
	MemoryContext				oldContext;
	ListCell 					*curItem;

	Assert(CdbComponentsContext);
	oldContext = MemoryContextSwitchTo(CdbComponentsContext);
	curItem = list_head(cdi->freelist);

	foreach(curItem, cdi->freelist)
	{
		segdbDesc = (SegmentDatabaseDescriptor *)lfirst(curItem);
		Assert(segdbDesc);

		if (segdbDesc->isWriter && !includeWriter)
			continue;

		cdi->freelist = foreach_delete_current(cdi->freelist, curItem);
		DECR_COUNT(cdi, numIdleQEs);

		cdbconn_termSegmentDescriptor(segdbDesc);
	}

	MemoryContextSwitchTo(oldContext);
}

void
cdbcomponent_cleanupIdleQEs(bool includeWriter)
{
	CdbComponentDatabases	*cdbs;
	int						i;

	/* use cdb_component_dbs directly */
	cdbs = cdb_component_dbs;

	if (cdbs == NULL)		
		return;

	if (cdbs->segment_db_info != NULL)
	{
		for (i = 0; i < cdbs->total_segment_dbs; i++)
		{
			CdbComponentDatabaseInfo *cdi = &cdbs->segment_db_info[i];
			cleanupComponentIdleQEs(cdi, includeWriter);
		}
	}

	if (cdbs->entry_db_info != NULL)
	{
		for (i = 0; i < cdbs->total_entry_dbs; i++)
		{
			CdbComponentDatabaseInfo *cdi = &cdbs->entry_db_info[i];
			cleanupComponentIdleQEs(cdi, includeWriter);
		}
	}

	return;
}

/*
 * This function is called when a transaction is started and the snapshot of
 * segments info will not changed until the end of transaction
 */
void
cdbcomponent_updateCdbComponents(void)
{
	uint8 ftsVersion= getFtsVersion();
	int expandVersion = GetGpExpandVersion();

	/*
	 * FTS takes responsibility for updating gp_segment_configuration, in each
	 * fts probe cycle, FTS firstly gets a copy of current configuration, then
	 * probe the segments based on it and finally free the copy in the end. In
	 * the probe stage, FTS might start/close transactions many times, so FTS
	 * should not update current copy of gp_segment_configuration when a new
	 * transaction is started.
	 */
	if (am_ftsprobe)
		return;

	PG_TRY();
	{
		if (cdb_component_dbs == NULL)
		{
			cdb_component_dbs = getCdbComponentInfo();
			cdb_component_dbs->fts_version = ftsVersion;
			cdb_component_dbs->expand_version = GetGpExpandVersion();
		}
		else if ((cdb_component_dbs->fts_version != ftsVersion ||
				 cdb_component_dbs->expand_version != expandVersion))
		{
			if (TempNamespaceOidIsValid())
			{
				/*
				 * Do not update here, otherwise, temp files will be lost
				 * in segments;
				 */
			}
			else
			{
				ELOG_DISPATCHER_DEBUG("FTS rescanned, get new component databases info.");
				cdbcomponent_destroyCdbComponents();
				cdb_component_dbs = getCdbComponentInfo();
				cdb_component_dbs->fts_version = ftsVersion;
				cdb_component_dbs->expand_version = expandVersion;
			}
		}
	}
	PG_CATCH();
	{
		FtsNotifyProber();

		PG_RE_THROW();
	}
	PG_END_TRY();

	Assert(cdb_component_dbs->numActiveQEs == 0);
}

/*
 * cdbcomponent_getCdbComponents
 *
 *
 * Storage for the SegmentInstances block and all subsidiary
 * structures are allocated from the caller's context.
 */
CdbComponentDatabases *
cdbcomponent_getCdbComponents()
{
	PG_TRY();
	{
		if (cdb_component_dbs == NULL)
		{
			cdb_component_dbs = getCdbComponentInfo();
			cdb_component_dbs->fts_version = getFtsVersion();
			cdb_component_dbs->expand_version = GetGpExpandVersion();
		}
	}
	PG_CATCH();
	{
		if (Gp_role == GP_ROLE_DISPATCH)
			FtsNotifyProber();

		PG_RE_THROW();
	}
	PG_END_TRY();

	return cdb_component_dbs;
}

/*
 * cdbcomponet_destroyCdbComponents
 *
 * Disconnect and destroy all idle QEs, releases the memory
 * occupied by the CdbComponentDatabases
 *
 * callers must clean up QEs used by dispatcher states.
 */
void
cdbcomponent_destroyCdbComponents(void)
{
	/* caller must clean up all segdbs used by dispatcher states */
	Assert(!cdbcomponent_activeQEsExist());

	hash_destroy(segment_ip_cache_htab);
	segment_ip_cache_htab = NULL;

	/* disconnect and destroy idle QEs include writers */
	cdbcomponent_cleanupIdleQEs(true);

	/* delete the memory context */
	if (CdbComponentsContext)
		MemoryContextDelete(CdbComponentsContext);	
	CdbComponentsContext = NULL;
	cdb_component_dbs = NULL;
}

/*
 * Allocated a segdb
 *
 * If there is idle segdb in the freelist, return it, otherwise, initialize
 * a new segdb.
 *
 * idle segdbs has an established connection with segment, but new segdb is
 * not setup yet, callers need to establish the connection by themselves.
 */
SegmentDatabaseDescriptor *
cdbcomponent_allocateIdleQE(int contentId, SegmentType segmentType)
{
	SegmentDatabaseDescriptor	*segdbDesc = NULL;
	CdbComponentDatabaseInfo	*cdbinfo;
	ListCell 					*curItem;
	MemoryContext 				oldContext;
	bool						isWriter;

	cdbinfo = cdbcomponent_getComponentInfo(contentId);	

	oldContext = MemoryContextSwitchTo(CdbComponentsContext);

	/*
	 * Always try to pop from the head.  Make sure to push them back to head
	 * in cdbcomponent_recycleIdleQE().
	 */
	foreach(curItem, cdbinfo->freelist)
	{
		SegmentDatabaseDescriptor *tmp =
				(SegmentDatabaseDescriptor *)lfirst(curItem);
		Assert(tmp);

		if ((segmentType == SEGMENTTYPE_EXPLICT_WRITER && !tmp->isWriter) ||
			(segmentType == SEGMENTTYPE_EXPLICT_READER && tmp->isWriter))
			continue;

		cdbinfo->freelist = foreach_delete_current(cdbinfo->freelist, curItem);
		/* update numIdleQEs */
		DECR_COUNT(cdbinfo, numIdleQEs);

		segdbDesc = tmp;
		break;
	}

	if (!segdbDesc)
	{
		/*
		 * 1. for entrydb, it's never be writer.
		 * 2. for first QE, it must be a writer.
		 */
		isWriter = contentId == -1 ? false: (cdbinfo->numIdleQEs == 0 && cdbinfo->numActiveQEs == 0);
		segdbDesc = cdbconn_createSegmentDescriptor(cdbinfo, nextQEIdentifer(cdbinfo->cdbs), isWriter);
	}

	cdbconn_setQEIdentifier(segdbDesc, -1);

	cdbinfo->activelist = lcons(segdbDesc, cdbinfo->activelist);
	INCR_COUNT(cdbinfo, numActiveQEs);

	MemoryContextSwitchTo(oldContext);

	return segdbDesc;
}

static bool
cleanupQE(SegmentDatabaseDescriptor *segdbDesc)
{
	Assert(segdbDesc != NULL);

#ifdef FAULT_INJECTOR
	if (SIMPLE_FAULT_INJECTOR("cleanup_qe") == FaultInjectorTypeSkip)
		return false;
#endif

	/*
	 * if the process is in the middle of blowing up... then we don't do
	 * anything here.  making libpq and other calls can definitely result in
	 * things getting HUNG.
	 */
	if (proc_exit_inprogress)
		return false;

	if (cdbconn_isBadConnection(segdbDesc))
		return false;

	/* if segment is down, the gang can not be reused */
	if (FtsIsSegmentDown(segdbDesc->segment_database_info))
		return false;

	/* If a reader exceed the cached memory limitation, destroy it */
	if (!segdbDesc->isWriter &&
		(segdbDesc->conn->mop_high_watermark >> 20) > gp_vmem_protect_gang_cache_limit)
		return false;

	/* Note, we cancel all "still running" queries */
	if (!cdbconn_discardResults(segdbDesc, 20))
	{
		elog(LOG, "cleaning up seg%d while it is still busy", segdbDesc->segindex);
		return false;
	}

	/* QE is no longer associated with a slice. */
	cdbconn_setQEIdentifier(segdbDesc, /* slice index */ -1);	

	return true;
}

void
cdbcomponent_recycleIdleQE(SegmentDatabaseDescriptor *segdbDesc, bool forceDestroy)
{
	CdbComponentDatabaseInfo	*cdbinfo;
	MemoryContext				oldContext;	
	int							maxLen;
	bool						isWriter;

	Assert(cdb_component_dbs);
	Assert(CdbComponentsContext);

	cdbinfo = segdbDesc->segment_database_info;
	isWriter = segdbDesc->isWriter;

	/* update num of active QEs */
	Assert(list_member_ptr(cdbinfo->activelist, segdbDesc));
	cdbinfo->activelist = list_delete_ptr(cdbinfo->activelist, segdbDesc);
	DECR_COUNT(cdbinfo, numActiveQEs);

	oldContext = MemoryContextSwitchTo(CdbComponentsContext);

	if (forceDestroy || !cleanupQE(segdbDesc))
		goto destroy_segdb;

	/* If freelist length exceed gp_cached_gang_threshold, destroy it */
	maxLen = segdbDesc->segindex == -1 ?
					MAX_CACHED_1_GANGS : gp_cached_gang_threshold;
	if (!isWriter && list_length(cdbinfo->freelist) >= maxLen)
		goto destroy_segdb;

	/* Recycle the QE, put it to freelist */
	if (isWriter)
	{
		/* writer is always the header of freelist */
		segdbDesc->segment_database_info->freelist =
			lcons(segdbDesc, segdbDesc->segment_database_info->freelist);
	}
	else
	{
		int			lastWriterPos = -1;
		ListCell   *cell;

		/*
		 * In cdbcomponent_allocateIdleQE() readers are always popped from the
		 * head, so to restore the original order we must pushed them back to
		 * the head, and keep in mind readers must be put after the writers.
		 */

		for (cell = list_head(segdbDesc->segment_database_info->freelist);
			 cell && ((SegmentDatabaseDescriptor *) lfirst(cell))->isWriter;
			 lastWriterPos++, cell = lnext(segdbDesc->segment_database_info->freelist, cell)) ;

		if (lastWriterPos >= 0)
			segdbDesc->segment_database_info->freelist = list_insert_nth(segdbDesc->segment_database_info->freelist,
							lastWriterPos + 1, segdbDesc);
		else
			segdbDesc->segment_database_info->freelist =
				lcons(segdbDesc, segdbDesc->segment_database_info->freelist);
	}

	INCR_COUNT(cdbinfo, numIdleQEs);

	MemoryContextSwitchTo(oldContext);

	return;

destroy_segdb:

	cdbconn_termSegmentDescriptor(segdbDesc);

	if (isWriter)
	{
		markCurrentGxactWriterGangLost();
	}

	MemoryContextSwitchTo(oldContext);
}

static int
nextQEIdentifer(CdbComponentDatabases *cdbs)
{
	int result;

	if (!cdbs->freeCounterList)
		return cdbs->qeCounter++;

	result = linitial_int(cdbs->freeCounterList);
	cdbs->freeCounterList = list_delete_first(cdbs->freeCounterList);
	return result;
}

bool
cdbcomponent_qesExist(void)
{
	return !cdb_component_dbs ? false :
			(cdb_component_dbs->numIdleQEs > 0 || cdb_component_dbs->numActiveQEs > 0);
}

bool
cdbcomponent_activeQEsExist(void)
{
	return !cdb_component_dbs ? false : cdb_component_dbs->numActiveQEs > 0;
}

/*
 * Find CdbComponentDatabaseInfo in the array by segment index.
 */
CdbComponentDatabaseInfo *
cdbcomponent_getComponentInfo(int contentId)
{
	CdbComponentDatabaseInfo *cdbInfo = NULL;
	CdbComponentDatabases *cdbs;

	cdbs = cdbcomponent_getCdbComponents();

	if (contentId < -1 || contentId >= cdbs->total_segments)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("unexpected content id %d, should be [-1, %d]",
						contentId, cdbs->total_segments - 1)));
	/* entry db */
	if (contentId == -1)
	{
		Assert(cdbs->total_entry_dbs == 1 || cdbs->total_entry_dbs == 2);
		/*
		 * For a standby QD, get the last entry db which can be the first (on
		 * a replica cluster) or the second (on a mirrored cluster) entry.
		 */
		if (IS_HOT_STANDBY_QD())
			cdbInfo = &cdbs->entry_db_info[cdbs->total_entry_dbs - 1];
		else
			cdbInfo = &cdbs->entry_db_info[0];	

		return cdbInfo;
	}

	/* no mirror, segment_db_info is sorted by content id */
	if (cdbs->total_segment_dbs == cdbs->total_segments)
	{
		cdbInfo = &cdbs->segment_db_info[contentId];
		return cdbInfo;
	}

	/* with mirror, segment_db_info is sorted by content id */
	if (cdbs->total_segment_dbs != cdbs->total_segments)
	{
		Assert(cdbs->total_segment_dbs == cdbs->total_segments * 2);
		cdbInfo = &cdbs->segment_db_info[2 * contentId];

		/* use the other segment if it is not what the QD wants */
		if ((IS_HOT_STANDBY_QD() && SEGMENT_IS_ACTIVE_PRIMARY(cdbInfo)) 
						|| (!IS_HOT_STANDBY_QD() && !SEGMENT_IS_ACTIVE_PRIMARY(cdbInfo)))
			cdbInfo = &cdbs->segment_db_info[2 * contentId + 1];

		return cdbInfo;
	}

	return cdbInfo;
}

static void
ensureInterconnectAddress(void)
{
	/*
	 * If the address type is wildcard, there is no need to populate an unicast
	 * address in interconnect_address.
	 */
	if (Gp_interconnect_address_type == INTERCONNECT_ADDRESS_TYPE_WILDCARD)
	{
		interconnect_address = NULL;
		return;
	}

	Assert(Gp_interconnect_address_type == INTERCONNECT_ADDRESS_TYPE_UNICAST);

	/* If the unicast address has already been assigned, exit early. */
	if (interconnect_address)
		return;

	/*
	 * Retrieve the segment's gp_segment_configuration.address value, in order
	 * to setup interconnect_address
	 */

	if (GpIdentity.segindex >= 0)
	{
		Assert(Gp_role == GP_ROLE_EXECUTE);
		Assert(MyProcPort->laddr.addr.ss_family == AF_INET
				|| MyProcPort->laddr.addr.ss_family == AF_INET6);
		/*
		 * We assume that the QD, using the address in gp_segment_configuration
		 * as its destination IP address, connects to the segment/QE.
		 * So, the local address in the PORT can be used for interconnect.
		 */
		char local_addr[NI_MAXHOST];
		getnameinfo((const struct sockaddr *)&MyProcPort->laddr.addr,
					MyProcPort->laddr.salen,
					local_addr, sizeof(local_addr),
					NULL, 0, NI_NUMERICHOST);
		interconnect_address = MemoryContextStrdup(TopMemoryContext, local_addr);
	}
	else if (Gp_role == GP_ROLE_DISPATCH)
	{
		/*
		 * Here, we can only retrieve the ADDRESS in gp_segment_configuration
		 * from `cdbcomponent*`. We couldn't get it in a way as the QEs.
		 */
		CdbComponentDatabaseInfo *qdInfo;
		qdInfo = cdbcomponent_getComponentInfo(MASTER_CONTENT_ID);
		interconnect_address = MemoryContextStrdup(TopMemoryContext, qdInfo->config->hostip);
	}
	else if (qdHostname && qdHostname[0] != '\0')
	{
		Assert(Gp_role == GP_ROLE_EXECUTE);
		/*
		 * QE on the master can't get its interconnect address like that on the primary.
		 * The QD connects to its postmaster via the unix domain socket.
		 */
		interconnect_address = qdHostname;
	}
	Assert(interconnect_address && strlen(interconnect_address) > 0);
}
/*
 * performs all necessary setup required for Apache Cloudberry mode.
 *
 * This includes cdblink_setup() and initializing the Motion Layer.
 */
void
cdb_setup(void)
{
	elog(DEBUG1, "Initializing Cloudberry components...");

	if (Gp_role != GP_ROLE_UTILITY && !IS_SINGLENODE())
	{
		if (!CurrentMotionIPCLayer) {
			ereport(ERROR,
				(errmsg("Interconnect moudle have not been preloaded"),
				 errdetail("Please make sure interconnect is included in option shared_preload_libraries")));
		}

		ensureInterconnectAddress();
		/* Initialize the Motion Layer IPC subsystem. */
		CurrentMotionIPCLayer->InitMotionLayerIPC();
	}

	/*
	 * Backend process requires consistent state, it cannot proceed until
	 * dtx recovery process finish up the recovery of distributed transactions.
	 *
	 * Ignore background worker because bgworker_should_start_mpp() already did
	 * the check.
	 *
	 * Ignore if we are the standby coordinator started in hot standby mode.
	 * We don't expect dtx recovery to have finished, as dtx recovery is
	 * performed at the end of startup. In hot standby, we are recovering
	 * continuously and should allow queries much earlier. Since a hot standby
	 * won't proceed dtx, it is not required to wait for recovery of the dtx
	 * that has been prepared but not committed (i.e. to commit them); on the
	 * other hand, the recovery of any in-doubt transactions (i.e. not prepared)
	 * won't bother a hot standby either, just like they can be recovered in the 
	 * background when a primary instance is running.
	 */
	if (!IsBackgroundWorker &&
		Gp_role == GP_ROLE_DISPATCH &&
		!*shmDtmStarted &&
		!IS_HOT_STANDBY_QD())
	{
		ereport(FATAL,
				(errcode(ERRCODE_CANNOT_CONNECT_NOW),
				 errmsg(POSTMASTER_IN_RECOVERY_MSG),
				 errdetail("waiting for distributed transaction recovery to complete")));
	}
}

/*
 * performs all necessary cleanup required when leaving Cloudberry
 * Database mode.  This is also called when the process exits.
 *
 * NOTE: the arguments to this function are here only so that we can
 *		 register it with on_proc_exit().  These parameters should not
 *		 be used since there are some callers to this that pass them
 *		 as NULL.
 *
 */
void
cdb_cleanup(int code pg_attribute_unused(), Datum arg
						pg_attribute_unused())
{
	elog(DEBUG1, "Cleaning up Cloudberry components...");

	DisconnectAndDestroyAllGangs(true);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		if (cdb_total_plans > 0)
		{
			elog(DEBUG1, "session dispatched %d plans %d slices (%f), largest plan %d",
				 cdb_total_plans, cdb_total_slices,
				 ((double) cdb_total_slices / (double) cdb_total_plans),
				 cdb_max_slices);
		}
	}

	if (Gp_role != GP_ROLE_UTILITY && !IS_SINGLENODE())
	{
		/* shutdown our listener socket */
		CurrentMotionIPCLayer->CleanUpMotionLayerIPC();
	}
}

/*
 * CdbComponentDatabaseInfoCompare:
 * A compare function for CdbComponentDatabaseInfo structs
 * that compares based on , isprimary desc
 * for use with qsort.
 */
static int
CdbComponentDatabaseInfoCompare(const void *p1, const void *p2)
{
	const CdbComponentDatabaseInfo *obj1 = (CdbComponentDatabaseInfo *) p1;
	const CdbComponentDatabaseInfo *obj2 = (CdbComponentDatabaseInfo *) p2;

	int			cmp = obj1->config->segindex - obj2->config->segindex;

	if (cmp == 0)
	{
		int			obj2cmp = 0;
		int			obj1cmp = 0;

		if (SEGMENT_IS_ACTIVE_PRIMARY(obj2))
			obj2cmp = 1;

		if (SEGMENT_IS_ACTIVE_PRIMARY(obj1))
			obj1cmp = 1;

		cmp = obj2cmp - obj1cmp;
	}

	return cmp;
}

/*
 * Maintain a cache of names.
 *
 * The keys are all NAMEDATALEN long.
 */
static char *
getDnsCachedAddress(char *name, int port, int elevel, bool use_cache)
{
	SegIpEntry	   *e = NULL;
	char			hostinfo[NI_MAXHOST];

	if (use_cache)
	{
		if (segment_ip_cache_htab == NULL)
		{
			HASHCTL		hash_ctl;

			MemSet(&hash_ctl, 0, sizeof(hash_ctl));

			hash_ctl.keysize = NAMEDATALEN + 1;
			hash_ctl.entrysize = sizeof(SegIpEntry);

			segment_ip_cache_htab = hash_create("segment_dns_cache",
												256, &hash_ctl, HASH_ELEM | HASH_STRINGS);
		}
		else
		{
			e = (SegIpEntry *) hash_search(segment_ip_cache_htab,
										   name, HASH_FIND, NULL);
			if (e != NULL)
				return e->hostinfo;
		}
	}

	/*
	 * The name is either not in our cache, or we've been instructed to not
	 * use the cache. Perform the name lookup.
	 */
	if (!use_cache || (use_cache && e == NULL))
	{
		MemoryContext oldContext = NULL;
		int			ret;
		char		portNumberStr[32];
		char	   *service;
		struct addrinfo *addrs = NULL,
				   *addr;
		struct addrinfo hint;

		/* Initialize hint structure */
		MemSet(&hint, 0, sizeof(hint));
		hint.ai_socktype = SOCK_STREAM;
		hint.ai_family = AF_UNSPEC;

		snprintf(portNumberStr, sizeof(portNumberStr), "%d", port);
		service = portNumberStr;

		ret = pg_getaddrinfo_all(name, service, &hint, &addrs);
		if (ret || !addrs)
		{
			if (addrs)
				pg_freeaddrinfo_all(hint.ai_family, addrs);

			/*
			 * If a host name is unknown, whether it is an error depends on its role:
			 * - if it is a primary then it's an error;
			 * - if it is a mirror then it's just a warning;
			 * but we do not know the role information here, so always treat it as a
			 * warning, the callers should check the role and decide what to do.
			 */
			if (ret != EAI_FAIL && elevel == ERROR)
				elevel = WARNING;

			ereport(elevel,
					(errmsg("could not translate host name \"%s\", port \"%d\" to address: %s",
							name, port, gai_strerror(ret))));

			return NULL;
		}

		/* save in the cache context */
		if (use_cache)
			oldContext = MemoryContextSwitchTo(TopMemoryContext);

		hostinfo[0] = '\0';
		for (addr = addrs; addr; addr = addr->ai_next)
		{
#ifdef HAVE_UNIX_SOCKETS
			/* Ignore AF_UNIX sockets, if any are returned. */
			if (addr->ai_family == AF_UNIX)
				continue;
#endif
			if (addr->ai_family == AF_INET) /* IPv4 address */
			{
				memset(hostinfo, 0, sizeof(hostinfo));
				pg_getnameinfo_all((struct sockaddr_storage *) addr->ai_addr, addr->ai_addrlen,
								   hostinfo, sizeof(hostinfo),
								   NULL, 0,
								   NI_NUMERICHOST);

				if (use_cache)
				{
					/* Insert into our cache htab */
					e = (SegIpEntry *) hash_search(segment_ip_cache_htab,
												   name, HASH_ENTER, NULL);
					memcpy(e->hostinfo, hostinfo, sizeof(hostinfo));
				}

				break;
			}
		}

#ifdef HAVE_IPV6

		/*
		 * IPv6 probably would work fine, we'd just need to make sure all the
		 * data structures are big enough for the IPv6 address.  And on some
		 * broken systems, you can get an IPv6 address, but not be able to
		 * bind to it because IPv6 is disabled or missing in the kernel, so
		 * we'd only want to use the IPv6 address if there isn't an IPv4
		 * address.  All we really need to do is test this.
		 */
		if (((!use_cache && !hostinfo[0]) || (use_cache && e == NULL))
			&& addrs->ai_family == AF_INET6)
		{
			addr = addrs;
			/* Get a text representation of the IP address */
			pg_getnameinfo_all((struct sockaddr_storage *) addr->ai_addr, addr->ai_addrlen,
							   hostinfo, sizeof(hostinfo),
							   NULL, 0,
							   NI_NUMERICHOST);

			if (use_cache)
			{
				/* Insert into our cache htab */
				e = (SegIpEntry *) hash_search(segment_ip_cache_htab,
											   name, HASH_ENTER, NULL);
				memcpy(e->hostinfo, hostinfo, sizeof(hostinfo));
			}
		}
#endif

		if (use_cache)
			MemoryContextSwitchTo(oldContext);

		pg_freeaddrinfo_all(hint.ai_family, addrs);
	}

	/* return a pointer to our cache. */
	if (use_cache)
		return e->hostinfo;

	return pstrdup(hostinfo);
}

/*
 * getDnsAddress
 *
 * same as getDnsCachedAddress, but without using the cache. A non-cached
 * version was used inline inside of cdbgang.c, and since it is needed now
 * elsewhere, it is available externally.
 */
char *
getDnsAddress(char *hostname, int port, int elevel)
{
	return getDnsCachedAddress(hostname, port, elevel, false);
}


/*
 * Given a component-db in the system, find the addresses at which it
 * can be reached, appropriately populate the argument-structure, and
 * maintain the ip-lookup-cache.
 */
static void
getAddressesForDBid(GpSegConfigEntry *c, int elevel)
{
	char	   *name;

	Assert(c != NULL);

	/* Use hostname */
	memset(c->hostaddrs, 0, COMPONENT_DBS_MAX_ADDRS * sizeof(char *));

#ifdef FAULT_INJECTOR
	if (am_ftsprobe &&
		SIMPLE_FAULT_INJECTOR("get_dns_cached_address") == FaultInjectorTypeSkip)
	{
		/* inject a dns error for primary of segment 0 */
		if (c->segindex == 0 &&
				c->preferred_role == GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY)
		{
			c->address = pstrdup("dnserrordummyaddress");
			c->hostname = pstrdup("dnserrordummyaddress");
		}
	}
#endif

	/*
	 * add an entry, using the first the "address" and then the "hostname" as
	 * fallback.
	 */
	name = getDnsCachedAddress(c->address, c->port, elevel, true);

	if (name)
	{
		c->hostaddrs[0] = pstrdup(name);
		return;
	}

	/* now the hostname. */
	name = getDnsCachedAddress(c->hostname, c->port, elevel, true);
	if (name)
	{
		c->hostaddrs[0] = pstrdup(name);
	}
	else
	{
		c->hostaddrs[0] = NULL;
	}

	return;
}

/*
 * hostPrimaryCountHashTableInit()
 *    Construct a hash table of HostPrimaryCountEntry
 */
static HTAB *
hostPrimaryCountHashTableInit(void)
{
	HASHCTL		info;

	/* Set key and entry sizes. */
	MemSet(&info, 0, sizeof(info));
	info.keysize = MAXHOSTNAMELEN;
	info.entrysize = sizeof(HostPrimaryCountEntry);

	return hash_create("HostSegs", 32, &info, HASH_ELEM | HASH_STRINGS);
}

/*
 * Given total number of primary segment databases and a number of
 * segments to "skip" - this routine creates a boolean map (array) the
 * size of total number of segments and randomly selects several
 * entries (total number of total_to_skip) to be marked as
 * "skipped". This is used for external tables with the 'gpfdist'
 * protocol where we want to get a number of *random* segdbs to
 * connect to a gpfdist client.
 *
 * Caller of this function should pfree skip_map when done with it.
 */
bool *
makeRandomSegMap(int total_primaries, int total_to_skip)
{
	int			randint;		/* some random int representing a seg    */
	int			skipped = 0;	/* num segs already marked to be skipped */
	bool	   *skip_map;

	skip_map = (bool *) palloc(total_primaries * sizeof(bool));
	MemSet(skip_map, false, total_primaries * sizeof(bool));

	while (total_to_skip != skipped)
	{
		/*
		 * create a random int between 0 and (total_primaries - 1).
		 */
		randint = cdbhashrandomseg(total_primaries);

		/*
		 * mark this random index 'true' in the skip map (marked to be
		 * skipped) unless it was already marked.
		 */
		if (skip_map[randint] == false)
		{
			skip_map[randint] = true;
			skipped++;
		}
	}

	return skip_map;
}

/*
 * Determine the dbid for the master standby
 */
int16
master_standby_dbid(void)
{
	int16		dbid = 0;
	HeapTuple	tup;
	Relation	rel;
	ScanKeyData scankey[2];
	SysScanDesc scan;

	/*
	 * Can only run on a master node, this restriction is due to the reliance
	 * on the gp_segment_configuration table.
	 */
	if (!IS_QUERY_DISPATCHER())
		elog(ERROR, "master_standby_dbid() executed on execution segment");

	/*
	 * SELECT * FROM gp_segment_configuration WHERE content = -1 AND role =
	 * GP_SEGMENT_CONFIGURATION_ROLE_MIRROR
	 */
	rel = table_open(GpSegmentConfigRelationId, AccessShareLock);
	ScanKeyInit(&scankey[0],
				Anum_gp_segment_configuration_content,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(-1));
	ScanKeyInit(&scankey[1],
				Anum_gp_segment_configuration_role,
				BTEqualStrategyNumber, F_CHAREQ,
				CharGetDatum(GP_SEGMENT_CONFIGURATION_ROLE_MIRROR));
	/* no index */
	scan = systable_beginscan(rel, InvalidOid, false,
							  NULL, 2, scankey);

	tup = systable_getnext(scan);

	if (HeapTupleIsValid(tup))
	{
		dbid = ((Form_gp_segment_configuration) GETSTRUCT(tup))->dbid;
		/* We expect a single result, assert this */
		Assert(systable_getnext(scan) == NULL);
	}

	systable_endscan(scan);
	/* no need to hold the lock, it's a catalog */
	table_close(rel, AccessShareLock);

	return dbid;
}

GpSegConfigEntry *
dbid_get_dbinfo(int16 dbid)
{
	HeapTuple	tuple;
	Relation	rel;
	ScanKeyData scankey;
	SysScanDesc scan;
	GpSegConfigEntry *i = NULL;

	/*
	 * Can only run on a master node, this restriction is due to the reliance
	 * on the gp_segment_configuration table.  This may be able to be relaxed
	 * by switching to a different method of checking.
	 */
	if (!IS_QUERY_DISPATCHER())
		elog(ERROR, "dbid_get_dbinfo() executed on execution segment");

	rel = heap_open(GpSegmentConfigRelationId, AccessShareLock);

	/* SELECT * FROM gp_segment_configuration WHERE dbid = :1 */
	ScanKeyInit(&scankey,
				Anum_gp_segment_configuration_dbid,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(dbid));
	scan = systable_beginscan(rel, GpSegmentConfigDbidWarehouseIndexId, true,
							  NULL, 1, &scankey);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Datum		attr;
		bool		isNull;
		Oid			warehouseid = InvalidOid;

		attr = heap_getattr(tuple, Anum_gp_segment_configuration_warehouseid,
							RelationGetDescr(rel), &isNull);
		Assert(!isNull);
		warehouseid = DatumGetObjectId(attr);

		if (!OidIsValid(warehouseid) || warehouseid == GetCurrentWarehouseId())
		{
			i = palloc(sizeof(GpSegConfigEntry));

			/*
			* dbid
			*/
			attr = heap_getattr(tuple, Anum_gp_segment_configuration_dbid,
								RelationGetDescr(rel), &isNull);
			Assert(!isNull);
			i->dbid = DatumGetInt16(attr);

			/*
			* content
			*/
			attr = heap_getattr(tuple, Anum_gp_segment_configuration_content,
								RelationGetDescr(rel), &isNull);
			Assert(!isNull);
			i->segindex = DatumGetInt16(attr);

			/*
			* role
			*/
			attr = heap_getattr(tuple, Anum_gp_segment_configuration_role,
								RelationGetDescr(rel), &isNull);
			Assert(!isNull);
			i->role = DatumGetChar(attr);

			/*
			* preferred-role
			*/
			attr = heap_getattr(tuple,
								Anum_gp_segment_configuration_preferred_role,
								RelationGetDescr(rel), &isNull);
			Assert(!isNull);
			i->preferred_role = DatumGetChar(attr);

			/*
			* mode
			*/
			attr = heap_getattr(tuple, Anum_gp_segment_configuration_mode,
								RelationGetDescr(rel), &isNull);
			Assert(!isNull);
			i->mode = DatumGetChar(attr);

			/*
			* status
			*/
			attr = heap_getattr(tuple, Anum_gp_segment_configuration_status,
								RelationGetDescr(rel), &isNull);
			Assert(!isNull);
			i->status = DatumGetChar(attr);

			/*
			* hostname
			*/
			attr = heap_getattr(tuple, Anum_gp_segment_configuration_hostname,
								RelationGetDescr(rel), &isNull);
			Assert(!isNull);
			i->hostname = TextDatumGetCString(attr);

			/*
			* address
			*/
			attr = heap_getattr(tuple, Anum_gp_segment_configuration_address,
								RelationGetDescr(rel), &isNull);
			Assert(!isNull);
			i->address = TextDatumGetCString(attr);

			/*
			* port
			*/
			attr = heap_getattr(tuple, Anum_gp_segment_configuration_port,
								RelationGetDescr(rel), &isNull);
			Assert(!isNull);
			i->port = DatumGetInt32(attr);

			break;
		}
	}
	if (i == NULL)
	{
		elog(ERROR, "could not find configuration entry for dbid %i", dbid);
	}

	systable_endscan(scan);
	heap_close(rel, NoLock);

	return i;
}

/*
 * Obtain the dbid of a of a segment at a given segment index (i.e., content id)
 * currently fulfilling the role specified. This means that the segment is
 * really performing the role of primary or mirror, irrespective of their
 * preferred role.
 */
int16
contentid_get_dbid(int16 contentid, char role, bool getPreferredRoleNotCurrentRole)
{
	int16		dbid = 0;
	Relation	rel;
	ScanKeyData scankey[3];
	int			nkeys = 2;
	SysScanDesc scan;
	HeapTuple	tup;

	/*
	 * Can only run on a master node, this restriction is due to the reliance
	 * on the gp_segment_configuration table.  This may be able to be relaxed
	 * by switching to a different method of checking.
	 */
	if (!IS_QUERY_DISPATCHER())
		elog(ERROR, "contentid_get_dbid() executed on execution segment");

	rel = heap_open(GpSegmentConfigRelationId, AccessShareLock);

	/* XXX XXX: CHECK THIS  XXX jic 2011/12/09 */
	if (getPreferredRoleNotCurrentRole)
	{
		/*
		 * SELECT * FROM gp_segment_configuration WHERE content = :1 AND
		 * preferred_role = :2
		 */
		ScanKeyInit(&scankey[0],
					Anum_gp_segment_configuration_content,
					BTEqualStrategyNumber, F_INT2EQ,
					Int16GetDatum(contentid));
		ScanKeyInit(&scankey[1],
					Anum_gp_segment_configuration_preferred_role,
					BTEqualStrategyNumber, F_CHAREQ,
					CharGetDatum(role));
		if (contentid != MASTER_CONTENT_ID)
		{
			nkeys++;
			ScanKeyInit(&scankey[2],
						Anum_gp_segment_configuration_warehouseid,
						BTEqualStrategyNumber, F_OIDEQ,
						ObjectIdGetDatum(GetCurrentWarehouseId()));
		}

		scan = systable_beginscan(rel, GpSegmentConfigContentPreferred_roleWarehouseIndexId, true,
								  NULL, nkeys, scankey);
	}
	else
	{
		/*
		 * SELECT * FROM gp_segment_configuration WHERE content = :1 AND role
		 * = :2
		 */
		ScanKeyInit(&scankey[0],
					Anum_gp_segment_configuration_content,
					BTEqualStrategyNumber, F_INT2EQ,
					Int16GetDatum(contentid));
		ScanKeyInit(&scankey[1],
					Anum_gp_segment_configuration_role,
					BTEqualStrategyNumber, F_CHAREQ,
					CharGetDatum(role));
		if (contentid != MASTER_CONTENT_ID)
		{
			nkeys++;
			ScanKeyInit(&scankey[2],
						Anum_gp_segment_configuration_warehouseid,
						BTEqualStrategyNumber, F_OIDEQ,
						ObjectIdGetDatum(GetCurrentWarehouseId()));
		}
		/* no index */
		scan = systable_beginscan(rel, InvalidOid, false,
								  NULL, nkeys, scankey);
	}

	tup = systable_getnext(scan);
	if (HeapTupleIsValid(tup))
	{
		dbid = ((Form_gp_segment_configuration) GETSTRUCT(tup))->dbid;
		/* We expect a single result, assert this */
		Assert(systable_getnext(scan) == NULL); /* should be only 1 */
	}

	/* no need to hold the lock, it's a catalog */
	systable_endscan(scan);
	heap_close(rel, AccessShareLock);

	return dbid;
}

List *
cdbcomponent_getCdbComponentsList(void)
{
	CdbComponentDatabases *cdbs;
	List *segments = NIL;
	int i;

	cdbs = cdbcomponent_getCdbComponents();

	for (i = 0; i < cdbs->total_segments; i++)
	{
		segments = lappend_int(segments, i);
	}

	return segments;
}


int16 
cdbcomponent_get_maxdbid(void)
{
	Relation	rel = table_open(GpSegmentConfigRelationId, AccessExclusiveLock);
	int16		dbid = 0;
	HeapTuple	tuple;
	SysScanDesc sscan;

	sscan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
	while ((tuple = systable_getnext(sscan)) != NULL)
	{
		dbid = Max(dbid,
				   ((Form_gp_segment_configuration) GETSTRUCT(tuple))->dbid);
	}
	systable_endscan(sscan);
	table_close(rel, NoLock);

	return dbid;
}

int16
cdbcomponent_get_availableDbId(void)
{
	/*
	 * Set up hash of used dbids.  We use int32 here because int16 doesn't
	 * have a convenient hash and we can use casting below to check for
	 * overflow of int16
	 */
	HASHCTL		hash_ctl;

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(int32);
	hash_ctl.entrysize = sizeof(int32);
	hash_ctl.hash = int32_hash;
	HTAB	   *htab = hash_create("Temporary table of dbids",
								   1024,
								   &hash_ctl,
								   HASH_ELEM | HASH_FUNCTION);

	/* scan GpSegmentConfigRelationId */
	Relation	rel = heap_open(GpSegmentConfigRelationId, AccessExclusiveLock);
	HeapTuple	tuple;
	SysScanDesc sscan;

	sscan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
	while ((tuple = systable_getnext(sscan)) != NULL)
	{
		int32		dbid = (int32) ((Form_gp_segment_configuration) GETSTRUCT(tuple))->dbid;

		(void) hash_search(htab, (void *) &dbid, HASH_ENTER, NULL);
	}
	systable_endscan(sscan);

	heap_close(rel, NoLock);

	/* search for available dbid */
	for (int32 dbid = 1;; dbid++)
	{
		if (dbid != (int16) dbid)
			elog(ERROR, "unable to find available dbid");

		if (hash_search(htab, (void *) &dbid, HASH_FIND, NULL) == NULL)
		{
			hash_destroy(htab);
			return dbid;
		}
	}
}

int16
cdbcomponent_get_maxcontentid(void)
{
	Relation	rel = heap_open(GpSegmentConfigRelationId, AccessExclusiveLock);
	int16		contentid = 0;
	HeapTuple	tuple;
	SysScanDesc sscan;

	sscan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
	while ((tuple = systable_getnext(sscan)) != NULL)
	{
		contentid = Max(contentid,
						((Form_gp_segment_configuration) GETSTRUCT(tuple))->content);
	}
	systable_endscan(sscan);
	heap_close(rel, NoLock);

	return contentid;
}

/*
 * return the number of total segments for current snapshot of
 * segments info
 */
int
getgpsegmentCount(void)
{
	/* 1 represents a singleton postgresql in utility mode */
	int32 numsegments = 1;

	if (Gp_role == GP_ROLE_DISPATCH)
		numsegments = cdbcomponent_getCdbComponents()->total_segments;
	else if (Gp_role == GP_ROLE_EXECUTE)
		numsegments = numsegmentsFromQD;
	/*
	 * If we are in 'Utility & Binary Upgrade' mode, it must be launched
	 * by the pg_upgrade, so we give it an correct numsegments to make
	 * sure the pg_upgrade can run normally.
	 * Only Utility QD process have the entire information in the
	 * gp_segment_configuration, so we count the segments count in this
	 * process.
	 */
	else if (Gp_role == GP_ROLE_UTILITY &&
			 IsBinaryUpgrade &&
			 IS_QUERY_DISPATCHER())
	{
		/*
		 * While upgrading in single node mode, we will get zero total
		 * segment count, which will cause assertion error in makeGpPolicy.
		 *
		 * Segment number should be set to a value not less than 1 anyway.
		 */
		numsegments = Max(cdbcomponent_getCdbComponents()->total_segments, 1);
	}

	return numsegments;
}

/*
 * IsOnConflictUpdate
 * Return true if a plannedstmt is an upsert: insert ... on conflict do update
 */
bool
IsOnConflictUpdate(PlannedStmt *ps)
{
	Plan      *plan;

	if (ps == NULL || ps->commandType != CMD_INSERT)
		return false;

	plan = ps->planTree;

	if (plan && IsA(plan, Motion))
		plan = outerPlan(plan);

	if (plan == NULL || !IsA(plan, ModifyTable))
		return false;

	return ((ModifyTable *)plan)->onConflictAction == ONCONFLICT_UPDATE;
}

/*
 * Avoid core file generation for this PANIC. It helps to avoid
 * filling up disks during tests and also saves time.
 */
void
AvoidCorefileGeneration()
{
#if defined(HAVE_GETRLIMIT) && defined(RLIMIT_CORE)
	struct rlimit lim;
	getrlimit(RLIMIT_CORE, &lim);
	lim.rlim_cur = 0;
	if (setrlimit(RLIMIT_CORE, &lim) != 0)
	{
		int			save_errno = errno;

		elog(NOTICE,
			 "setrlimit failed for RLIMIT_CORE soft limit to zero. errno: %d (%m).",
			 save_errno);
	}
#endif
}

PG_FUNCTION_INFO_V1(gp_get_suboverflowed_backends);
/*
 * Find the backends where subtransaction overflowed.
 */
Datum
gp_get_suboverflowed_backends(PG_FUNCTION_ARGS)
{
	int 			i;
	ArrayBuildState *astate = NULL;

	LWLockAcquire(ProcArrayLock, LW_SHARED);
	for (i = 0; i < ProcGlobal->allProcCount; i++)
	{
		if (ProcGlobal->subxidStates[i].overflowed)
			astate = accumArrayResult(astate,
									  Int32GetDatum(ProcGlobal->allProcs[i].pid),
									  false, INT4OID, CurrentMemoryContext);
	}
	LWLockRelease(ProcArrayLock);

	if (astate)
		PG_RETURN_DATUM(makeArrayResult(astate,
											CurrentMemoryContext));
	else
		PG_RETURN_NULL();
}

#else 
bool am_ftshandler = false;


typedef struct CdbConfigsCache {
	int counts;
	int buff_size;

	char buff[SEGMENT_CONFIGURATION_ONE_LINE_LENGTH * SEGMENT_CONFIGURATION_CACHE_COUNTS];
} CdbConfigsCache;

static CdbConfigsCache * cdb_config_cache = NULL;

Size
ShmemSegmentConfigsCacheSize(void)
{
	return sizeof(CdbConfigsCache);
}

void
ShmemSegmentConfigsCacheAllocation(void)
{
	Size size = ShmemSegmentConfigsCacheSize();

	cdb_config_cache = (CdbConfigsCache *) ShmemAlloc(size);
	/* Mark all slots as empty */
	MemSet(cdb_config_cache, 0, size);
}

void
ShmemSegmentConfigsCacheReset(void)
{
	LWLockAcquire(CdbConfigCacheLock, LW_EXCLUSIVE);
	if (!isSegmentConfigsCacheEnable())
	{
		LWLockRelease(CdbConfigCacheLock);
		return;
	}

	MemSet(cdb_config_cache, 0, ShmemSegmentConfigsCacheSize());
	LWLockRelease(CdbConfigCacheLock);
}

void
ShmemSegmentConfigsCacheForceFlush(void)
{
	int total_dbs = 0;

	if (!isSegmentConfigsCacheEnable())
		return;
	
	ShmemSegmentConfigsCacheReset();
	GpSegConfigEntry * configs = readGpSegConfigFromETCDAllowNull(&total_dbs);
	elog(DEBUG1, "Segment configs cache have been force flushed, total dbs=%d", total_dbs);
	cleanGpSegConfigs(configs, total_dbs);
}

static int
dumpSegConfigInfo(GpSegConfigEntry * config, char * buf)
{
	return sprintf(buf, "%d %d %c %c %c %c %d %s %s %s\n", config->dbid, 
			config->segindex, config->role, config->preferred_role,
			config->mode, config->status, config->port,
			config->hostname, config->address, config->datadir);
}

bool
isSegmentConfigsCacheEnable(void)
{
	return cdb_config_cache && gp_etcd_enable_cache;
}

bool
isSegmentConfigsCached(void)
{
	return cdb_config_cache && \
		gp_etcd_enable_cache && \
		cdb_config_cache->counts != 0 && \
		cdb_config_cache->buff_size != 0;
}

char *
readSegmentConfigsCache(int *total_dbs)
{
	char *buff = NULL;

	LWLockAcquire(CdbConfigCacheLock, LW_EXCLUSIVE);
	if (isSegmentConfigsCached())
	{
		/* Can't use pstrdup here */
		buff = palloc(cdb_config_cache->buff_size + 1);
		memcpy(buff, cdb_config_cache->buff, cdb_config_cache->buff_size); 
		*total_dbs = cdb_config_cache->counts;
	}
	else
		*total_dbs = 0;

	LWLockRelease(CdbConfigCacheLock);
	return buff;
}

void
writeSegmentConfigsCacheBuff(char * config_buff, int buff_length, int total_dbs)
{
	LWLockAcquire(CdbConfigCacheLock, LW_EXCLUSIVE);
	if (!isSegmentConfigsCacheEnable())
	{
		LWLockRelease(CdbConfigCacheLock);
		return;
	}

	/* FTS_TODO: support for cache expansion */ 
	if (buff_length >= SEGMENT_CONFIGURATION_ONE_LINE_LENGTH * SEGMENT_CONFIGURATION_CACHE_COUNTS)
	{
		gp_etcd_enable_cache = false;
		LWLockRelease(CdbConfigCacheLock);
		return;
	}

	memcpy(cdb_config_cache->buff, config_buff, buff_length);
	cdb_config_cache->buff_size = buff_length;
	cdb_config_cache->counts = total_dbs;
	LWLockRelease(CdbConfigCacheLock);
}

/*
 * Helper Functions
 */
static CdbComponentDatabases *getCdbComponentInfo(void);
static void cleanupComponentIdleQEs(CdbComponentDatabaseInfo *cdi, bool includeWriter);

static int	CdbComponentDatabaseInfoCompare(const void *p1, const void *p2);

static void getAddressesForDBid(GpSegConfigEntry *c, int elevel);
static HTAB *hostSegsHashTableInit(void);

static int nextQEIdentifer(CdbComponentDatabases *cdbs);

static HTAB *segment_ip_cache_htab = NULL;

int numsegmentsFromQD = -1;

typedef struct SegIpEntry
{
	char		key[NAMEDATALEN];
	char		hostinfo[NI_MAXHOST];
} SegIpEntry;

typedef struct HostSegsEntry
{
	char		hostip[INET6_ADDRSTRLEN];
	int			segmentCount;
} HostSegsEntry;

/* Fixed GUC */ 
bool     gp_etcd_enable_cache = true;
static   etcdlib_t * static_etcdlib = NULL;
char     *fts_dump_file_key = NULL;
char     *fts_standby_promote_ready_key = NULL;
char     *gp_etcd_account_id = NULL;
char     *gp_etcd_cluster_id = NULL;
char     *gp_etcd_namespace = NULL;
char     *gp_etcd_endpoints = NULL;

static etcdlib_endpoint_t etcd_endpoints[GP_ETCD_ENDPOINTS_NUM] = {0};
static int etcd_endpoints_num = 0;

static bool
etcdlib_init_config(etcdlib_endpoint_t *petcd_endpoints)
{
	bool ret = false;
	fts_dump_file_key = (char *)palloc0(sizeof(char)*GP_ETCD_KEY_LEN);
	ret = generateGPSegConfigKey(fts_dump_file_key, gp_etcd_namespace, gp_etcd_account_id, gp_etcd_cluster_id, 
						gp_etcd_endpoints, petcd_endpoints, &etcd_endpoints_num);
	if (!ret)
	{
		elog(ERROR, "failed to init ETCD config for fts dump key: %s", fts_dump_file_key);
		return false;
	}

	fts_standby_promote_ready_key = (char *)palloc0(sizeof(char)*GP_ETCD_KEY_LEN);
	generateGPFtsPromoteReadyKey(fts_standby_promote_ready_key, gp_etcd_namespace, gp_etcd_account_id, gp_etcd_cluster_id);
	if (fts_standby_promote_ready_key[0] == '\0')
	{
		elog(ERROR, "failed to init ETCD config for fts standby promote key: %s", fts_standby_promote_ready_key);
		return false;
	}
	elog(LOG, "etcdlib_init_config successfully initialized with fts_dump_file_key: %s, fts_standby_promote_ready_key:%s",
			fts_dump_file_key, fts_standby_promote_ready_key);
	return true;
}

static etcdlib_t *
get_etcdlib(void)
{
	/* No need lock here
	 * Cause etcdlib will be inited after postmaster start
	 */ 
	if (unlikely(static_etcdlib == NULL))
	{
		bool isEtcdKeyConfigured = false;
		MemoryContext oldContext;
		oldContext = MemoryContextSwitchTo(TopMemoryContext);
		etcdlib_endpoint_t *petcd_endpoints = etcd_endpoints;
		isEtcdKeyConfigured = etcdlib_init_config(petcd_endpoints);
		Assert(isEtcdKeyConfigured != false);
		static_etcdlib = etcdlib_create(petcd_endpoints, etcd_endpoints_num, 0);
		if (!static_etcdlib)
			ereport(ERROR, errmsg("Cannot connect to ETCD. [gp_etcd_host=%s, gp_etcd_port=%d]", static_etcdlib->host, static_etcdlib->port));
		MemoryContextSwitchTo(oldContext);
	}

	return static_etcdlib;
}

bool
setStandbyPromoteReady(bool standby_promote_ready)
{
	etcdlib_t *etcdlib = NULL;
	int rc = 0;

	etcdlib = get_etcdlib();

	rc = etcdlib_set(etcdlib, fts_standby_promote_ready_key,
		standby_promote_ready ? FTS_STANDBY_PROMOTE_READY : FTS_STANDBY_PROMOTE_NO_READY
		, 0, false);
	if (rc != 0)
	{
		elog(ERROR,
				"fail to write standby_promote_ready info into ETCD. rc=%d", rc);
		return false;
	}
	return true;
}

bool
getStandbyPromoteReady(bool *standby_promote_ready)
{
	etcdlib_t *etcdlib = NULL;
	int rc = 0;
	char *buff = NULL;

	etcdlib = get_etcdlib();

	rc = etcdlib_get(etcdlib, fts_standby_promote_ready_key, &buff, NULL);
	if (rc != 0 || !buff)
	{
		if (buff)
			pfree(buff);
		elog(ERROR,
			"fail to read standby_promote_ready info into ETCD. rc=%d", rc);
		return false;
	}

	rc = strncmp(buff, FTS_STANDBY_PROMOTE_READY, FTS_STANDBY_PROMOTE_LEN);

	if (strlen(buff) != FTS_STANDBY_PROMOTE_LEN || 
		(rc != 0
		&& strncmp(buff, FTS_STANDBY_PROMOTE_NO_READY, FTS_STANDBY_PROMOTE_LEN) != 0))
	{
		pfree(buff);
		elog(ERROR, "invalid standby_promote_ready in ETCD. value=%s", buff);
		return false;
	}

	*standby_promote_ready = rc == 0;

	pfree(buff);
	return true;
}

GpSegConfigEntry * 
readGpSegConfigFromETCDAllowNull(int *total_dbs)
{
	return readGpSegConfigFromETCD(total_dbs, true);
}

GpSegConfigEntry *
readGpSegConfig(char * buff, int *total_dbs)
{
	int	idx = 0;
	int	array_size = SEGMENT_CONFIGURATION_ALLOC_COUNTS;
	GpSegConfigEntry *configs = NULL;
	GpSegConfigEntry *config = NULL;
	int rc;

	char	hostname[MAXHOSTNAMELEN];
	char	address[MAXHOSTNAMELEN];
	char	datadir[4096];

	if (!buff)
		ereport(ERROR, errmsg("invalid buffer which is NULL"));

	configs = palloc0(sizeof (GpSegConfigEntry) * array_size);

	char * ptr = strtok(buff, "\n");
	while (ptr)
	{

		config = &configs[idx];

		rc = sscanf(ptr, "%d %d %c %c %c %c %d %s %s %s", (int *)&config->dbid, (int *)&config->segindex,
				   &config->role, &config->preferred_role, &config->mode, &config->status,
				   &config->port, hostname, address, datadir);

		if (rc != GPSEGCONFIGNUMATTR)
		{
			cleanGpSegConfigs(configs, idx);
			ereport(ERROR,
					errmsg("cannot decode config info from ETCD. sscanf rc=%d", rc));
		}

		config->hostname = pstrdup(hostname);
		config->address = pstrdup(address);
		config->datadir = pstrdup(datadir);

		idx++;

		if (idx >= array_size)
		{
			array_size = array_size * 2;
			configs = (GpSegConfigEntry *)
				repalloc(configs, sizeof(GpSegConfigEntry) * array_size);
			size_t expand_size = (sizeof(GpSegConfigEntry) * array_size / 2);
			memset(configs + expand_size, 0, expand_size);
		}

		ptr = strtok(NULL, "\n");
	}

	if (total_dbs)
		*total_dbs = idx;

	return configs;
}

GpSegConfigEntry *
readGpSegConfigFromETCD(int *total_dbs, bool allow_null)
{
	char *buff = NULL;
	char *buff_cpy = NULL;
	int rc = 0;
	GpSegConfigEntry *configs = NULL;
	etcdlib_t *etcdlib = NULL;
	bool cache_hit = false;
	int total_dbs_parsed = 0;

	buff = readSegmentConfigsCache(total_dbs);
	if (!buff)
	{
		etcdlib = get_etcdlib();
		rc = etcdlib_get(etcdlib, fts_dump_file_key, &buff, NULL);
		if (rc != 0)
		{
			if (buff == NULL && allow_null)
			{
				*total_dbs = 0;
				return configs;
			}
			else
				ereport(ERROR,
						errmsg("fail to get fts info from ETCD. rc=%d", rc));
		}
	}
	else
		cache_hit = true;

	if (cache_hit)
	{
		configs = readGpSegConfig(buff, &total_dbs_parsed);
		if (total_dbs_parsed != *total_dbs)
		{
			cleanGpSegConfigs(configs, total_dbs_parsed);
			pfree(buff);
			ereport(ERROR,
					errmsg("segment configuration cache got some error. total_dbs_parsed=%d, cached_total_dbs=%d", total_dbs_parsed, *total_dbs));
		}
	}
	else
	{
		buff_cpy = pstrdup(buff);
		configs = readGpSegConfig(buff_cpy, total_dbs);
		writeSegmentConfigsCacheBuff(buff, strlen(buff) + 1, *total_dbs);
		pfree(buff_cpy);
	}
	
	pfree(buff);
	return configs;
}

void
cleanGpSegConfigs(GpSegConfigEntry *configs, int total_dbs)
{
	if (configs)
	{
		GpSegConfigEntry *config;
		for (int i = 0; i < total_dbs; i++)
		{
			config = &configs[i];
			if (config->hostname)
				pfree(config->hostname);
			if (config->address)
				pfree(config->address);
			if (config->datadir)
				pfree(config->datadir);
		}

		pfree(configs);
		configs = NULL;
	}
}

static 
void copyGpsegConfig(GpSegConfigEntry *src, GpSegConfigEntry *dst)
{
	Assert(src);
	Assert(dst);

	dst->dbid = src->dbid;
	dst->segindex = src->segindex;
	dst->role = src->role;
	dst->preferred_role = src->preferred_role;
	dst->mode = src->mode;
	dst->status = src->status;
	dst->port = src->port;
	dst->hostname = pstrdup(src->hostname);
	dst->address = pstrdup(src->address);
	dst->datadir = pstrdup(src->datadir);
}


/*
 * Master function for adding a new segment
 */
void
addSegment(GpSegConfigEntry *new_segment_information)
{
	int dump_bytes = 0;
	int rc = 0;
	char* buff = NULL;
	char* buff_ptr = NULL;
	GpSegConfigEntry* configs = NULL;
	GpSegConfigEntry* config = NULL;
	int total_dbs;
	Relation	rel;
	etcdlib_t *etcdlib;

	etcdlib = get_etcdlib();
	rel = heap_open(RelnameGetRelid(GpSegmentConfigRelationName), AccessShareLock);

	configs = readGpSegConfigFromETCD(&total_dbs, true);

	buff = palloc0(SEGMENT_CONFIGURATION_ONE_LINE_LENGTH * (total_dbs + 1));
	buff_ptr = buff;

	for (int index = 0; index < total_dbs; index++)
	{
		config = &configs[index];
		if (config->dbid == new_segment_information->dbid)
		{
			cleanGpSegConfigs(configs, total_dbs);
			pfree(buff);
			heap_close(rel, NoLock);
			ereport(ERROR,
					errmsg("already exist dbid=%d. should not add it twice.", new_segment_information->dbid));
		}

		dump_bytes = dumpSegConfigInfo(config, buff_ptr);
		if (dump_bytes <= 0)
		{
			cleanGpSegConfigs(configs, total_dbs);
			pfree(buff);
			heap_close(rel, NoLock);
			ereport(ERROR,
				errmsg("fail to dump data info to string, dbid=%d, content=%d", 
					config->dbid, config->segindex));
		}
		buff_ptr = buff_ptr + dump_bytes;
	}

	cleanGpSegConfigs(configs, total_dbs);

	dump_bytes = dumpSegConfigInfo(new_segment_information, buff_ptr);
	if (dump_bytes <= 0)
	{
		pfree(buff);
		heap_close(rel, NoLock);
		ereport(ERROR,
			errmsg("fail to dump new add segment info to string, dbid=%d, content=%d", 
				new_segment_information->dbid, new_segment_information->segindex));
	}

	rc = etcdlib_set(etcdlib, fts_dump_file_key, buff, 0, false);
	if (rc != 0)
	{
		pfree(buff);
		heap_close(rel, NoLock);
		ereport(ERROR,
				errmsg("fail to write fts info into ETCD. rc=%d", rc));
	}
	
	writeSegmentConfigsCacheBuff(buff, strlen(buff) + 1, total_dbs + 1);
	pfree(buff);
	heap_close(rel, NoLock);
}

/*
 * Master function to remove a segment from all catalogs
 */
void
delSegment(int16 dbid)
{
	int rc = 0;
	int dump_bytes = 0;
	char *rewrite_buf = NULL; 
	char *rewrite_ptr = NULL;
	GpSegConfigEntry *configs = NULL;
	GpSegConfigEntry *config = NULL;
	int total_dbs = 0;
	int index = 0;
	int index_rm = -1;
	Relation	rel;
	etcdlib_t *etcdlib;
	
	etcdlib = get_etcdlib();
	rel = heap_open(RelnameGetRelid(GpSegmentConfigRelationName), AccessShareLock);

	configs = readGpSegConfigFromETCD(&total_dbs, false);

	rewrite_buf = palloc0(SEGMENT_CONFIGURATION_ONE_LINE_LENGTH * (total_dbs - 1));
	rewrite_ptr = rewrite_buf;
	for (index = 0; index < total_dbs; index++)
	{
		config = &configs[index];
		if (config->dbid == dbid)
			index_rm = index;
		else
		{
			dump_bytes = dumpSegConfigInfo(config, rewrite_ptr);
			if (dump_bytes <= 0)
			{
				cleanGpSegConfigs(configs, total_dbs);
				pfree(rewrite_buf);
				heap_close(rel, NoLock);
				ereport(ERROR,
					errmsg("fail to dump data info to string, dbid=%d, content=%d", 
						config->dbid, config->segindex));
			}

			rewrite_ptr = rewrite_ptr + dump_bytes;
		}
	}

	cleanGpSegConfigs(configs, total_dbs);

	/* segment not found */ 
	if (index_rm == -1)
	{
		pfree(rewrite_buf);
		heap_close(rel, NoLock);
		ereport(ERROR,
					errmsg("fail to find [dbid=%d] in FTS_DUMP_FILE_KEY", dbid));
	}

	rc = etcdlib_set(etcdlib, fts_dump_file_key, rewrite_buf, 0, false);
	if (rc != 0)
	{
		pfree(rewrite_buf);
		heap_close(rel, NoLock);
		ereport(ERROR,
				errmsg("fail to write fts info into ETCD. rc=%d", rc));
	}

	writeSegmentConfigsCacheBuff(rewrite_buf, strlen(rewrite_buf) + 1, total_dbs - 1);

	pfree(rewrite_buf);
	heap_close(rel, NoLock);
}

void
cleanSegments(void)
{
	Relation   rel;
	etcdlib_t *etcdlib;
	
	etcdlib = get_etcdlib();
	rel = heap_open(RelnameGetRelid(GpSegmentConfigRelationName), AccessShareLock);

	etcdlib_del(etcdlib, fts_dump_file_key);
	ShmemSegmentConfigsCacheReset();

	heap_close(rel, NoLock);
}

void
rewriteSegments(char *rewrite_string, bool allow_diff_db_counts)
{
	int rc;
	etcdlib_t *etcdlib = NULL;
	GpSegConfigEntry *configs;
	GpSegConfigEntry *tmp_configs;
	int total_dbs;
	int total_dbs_in_etcd;
	char *rewrite_string_cpy;
	Relation	rel;

	etcdlib = get_etcdlib();
	rel = heap_open(RelnameGetRelid(GpSegmentConfigRelationName), AccessShareLock);

	rewrite_string_cpy = pstrdup(rewrite_string);
	configs = readGpSegConfig(rewrite_string_cpy, &total_dbs);

	if (!allow_diff_db_counts)
	{
		tmp_configs = readGpSegConfigFromETCDAllowNull(&total_dbs_in_etcd);
		cleanGpSegConfigs(tmp_configs, total_dbs_in_etcd);

		/* ff current segment counts not match the FTS results 
		 * it may means that during `ftsprobe`, user additionally operate some tools(like gpinitstanby)
		 * which will make segment counts change. If force to override segment info here, 
		 * may cause loss of some segment record.
		 */
		if (total_dbs_in_etcd != total_dbs)
		{
			pfree(rewrite_string_cpy);
			heap_close(rel, NoLock);
			elog(WARNING, "Ignored update from fts, Current segments counts changed.");
			return;
		}
	}

	if (!configs || total_dbs == 0)
	{
		pfree(rewrite_string_cpy);
		heap_close(rel, NoLock);
		elog(WARNING, "fail to parse rewrite segment infos: \"%s\"", rewrite_string);
		return;
	}

	cleanGpSegConfigs(configs, total_dbs);
	pfree(rewrite_string_cpy);

	rc = etcdlib_set(etcdlib, fts_dump_file_key, rewrite_string, 0, false);
	if (rc != 0)
	{
		heap_close(rel, NoLock);
		ereport(ERROR,
				errmsg("fail to write fts info from ETCD. rc=%d", rc));
	}

	writeSegmentConfigsCacheBuff(rewrite_string, strlen(rewrite_string) + 1, total_dbs);
	
	heap_close(rel, NoLock);
}

void
updateSegmentModeStatus(int16 dbid, char mode, char status)
{
	int rc = 0;
	int dump_bytes = 0;
	etcdlib_t *etcdlib = NULL;
	Relation	rel;
	int total_dbs = 0;
	char *rewrite_buf = NULL;
	char *rewrite_ptr = NULL;
	GpSegConfigEntry *configs = NULL;
	GpSegConfigEntry *config = NULL;
	int index;
	bool found = false;

	etcdlib = get_etcdlib();
	rel = heap_open(RelnameGetRelid(GpSegmentConfigRelationName), AccessShareLock);

	configs = readGpSegConfigFromETCD(&total_dbs, false);

	rewrite_buf = palloc0(SEGMENT_CONFIGURATION_ONE_LINE_LENGTH * (total_dbs - 1));
	rewrite_ptr = rewrite_buf;
	for (index = 0; index < total_dbs; index++)
	{
		config = &configs[index];
		if (config->dbid == dbid)
		{
			found = true;
			configs->mode = mode;
			configs->status = status;
		} 

		dump_bytes = dumpSegConfigInfo(config, rewrite_ptr);
		if (dump_bytes <= 0)
		{
			cleanGpSegConfigs(configs, total_dbs);
			pfree(rewrite_buf);
			heap_close(rel, NoLock);
			ereport(ERROR,
				errmsg("fail to dump data info to string, dbid=%d, content=%d", 
					config->dbid, config->segindex));
		}

		rewrite_ptr = rewrite_ptr + dump_bytes;
	}
	cleanGpSegConfigs(configs, total_dbs);

	if (!found)
	{
		pfree(rewrite_buf);
		heap_close(rel, NoLock);
		ereport(ERROR,
					errmsg("fail to find [dbid=%d] in FTS_DUMP_FILE_KEY", dbid));
	}

	rc = etcdlib_set(etcdlib, fts_dump_file_key, rewrite_buf, 0, false);
	if (rc != 0)
	{
		pfree(rewrite_buf);
		heap_close(rel, NoLock);
		ereport(ERROR,
				errmsg("fail to write fts info into ETCD. rc=%d", rc));
	}

	writeSegmentConfigsCacheBuff(rewrite_buf, strlen(rewrite_buf) + 1, total_dbs);

	pfree(rewrite_buf);
	heap_close(rel, NoLock);
}

void
activateStandby(int16 standby_dbid, int16 primary_dbid)
{
	int rc = 0;
	int dump_bytes = 0;
	char *rewrite_buf = NULL; 
	char *rewrite_ptr = NULL;
	GpSegConfigEntry *configs = NULL;
	GpSegConfigEntry *config = NULL;
	int total_dbs;
	int index;
	int index_rm = -1;
	bool found_standby = false;
	etcdlib_t *etcdlib;
	Relation	rel;

	etcdlib = get_etcdlib();
	rel = heap_open(RelnameGetRelid(GpSegmentConfigRelationName), AccessShareLock);

	configs = readGpSegConfigFromETCD(&total_dbs, false);

	rewrite_buf = palloc0(SEGMENT_CONFIGURATION_ONE_LINE_LENGTH * (total_dbs - 1));
	rewrite_ptr = rewrite_buf;
	for (index = 0; index < total_dbs; index++)
	{
		config = &configs[index];

		if (config->dbid == primary_dbid)
		{
			index_rm = index;
			continue;
		}

		if (config->dbid == standby_dbid)
		{
			found_standby = true;
			config->role = GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;
			config->preferred_role = GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY;
		}

		dump_bytes = dumpSegConfigInfo(config, rewrite_ptr);
		if (dump_bytes <= 0)
		{
			cleanGpSegConfigs(configs, total_dbs);
			pfree(rewrite_buf);
			heap_close(rel, NoLock);
			ereport(ERROR,
				errmsg("fail to dump data info to string, dbid=%d, content=%d", 
					config->dbid, config->segindex));
		}

		rewrite_ptr = rewrite_ptr + dump_bytes;
	}

	cleanGpSegConfigs(configs, total_dbs);

	if (index_rm == -1 || !found_standby)
	{
		pfree(rewrite_buf);
		heap_close(rel, NoLock);
		ereport(ERROR,
					errmsg("fail to find primary_dbid=%d or standby_dbid=%d in FTS_DUMP_FILE_KEY", 
					primary_dbid,standby_dbid));
	}

	rc = etcdlib_set(etcdlib, fts_dump_file_key, rewrite_buf, 0, false);
	if (rc != 0)
	{
		pfree(rewrite_buf);
		heap_close(rel, NoLock);
		ereport(ERROR,
				errmsg("fail to write fts info into ETCD. rc=%d", rc));
	}

	writeSegmentConfigsCacheBuff(rewrite_buf, strlen(rewrite_buf) + 1, total_dbs - 1);

	pfree(rewrite_buf);
	heap_close(rel, NoLock);
}

/*
 *  Internal function to initialize each component info
 */
static CdbComponentDatabases *
getCdbComponentInfo(void)
{
	MemoryContext oldContext;
	CdbComponentDatabaseInfo *cdbInfo;
	CdbComponentDatabases *component_databases = NULL;
	GpSegConfigEntry *configs;

	int			i;
	int			x = 0;
	int			total_dbs = 0;

	bool		found;
	HostSegsEntry *hsEntry;

	if (!CdbComponentsContext)
		CdbComponentsContext = AllocSetContextCreate(TopMemoryContext, "cdb components Context",
								ALLOCSET_DEFAULT_MINSIZE,
								ALLOCSET_DEFAULT_INITSIZE,
								ALLOCSET_DEFAULT_MAXSIZE);

	oldContext = MemoryContextSwitchTo(CdbComponentsContext);

	HTAB	   *hostSegsHash = hostSegsHashTableInit();

	configs = readGpSegConfigFromETCD(&total_dbs, false);
	component_databases = palloc0(sizeof(CdbComponentDatabases));

	component_databases->numActiveQEs = 0;
	component_databases->numIdleQEs = 0;
	component_databases->qeCounter = 0;
	component_databases->freeCounterList = NIL;

	component_databases->segment_db_info =
		(CdbComponentDatabaseInfo *) palloc0(sizeof(CdbComponentDatabaseInfo) * total_dbs);

	component_databases->entry_db_info =
		(CdbComponentDatabaseInfo *) palloc0(sizeof(CdbComponentDatabaseInfo) * 2);

	for (i = 0; i < total_dbs; i++)
	{
		CdbComponentDatabaseInfo	*pRow;
		GpSegConfigEntry	*config = &configs[i];

		/* lookup hostip/hostaddrs cache */
		config->hostip= NULL;
		getAddressesForDBid(config, ERROR);

		/*
		 * We make sure we get a valid hostip for primary here,
		 * if hostip for mirrors can not be get, ignore the error.
		 */
		if (config->hostaddrs[0] == NULL &&
			config->role == GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					errmsg("cannot resolve network address for dbid=%d", config->dbid)));

		if (config->hostaddrs[0] != NULL)
			config->hostip = pstrdup(config->hostaddrs[0]);
		AssertImply(config->hostip, strlen(config->hostip) <= INET6_ADDRSTRLEN);

		/*
		 * Determine which array to place this rows data in: entry or segment,
		 * based on the content field.
		 */
		if (config->segindex >= 0)
		{
			pRow = &component_databases->segment_db_info[component_databases->total_segment_dbs];
			component_databases->total_segment_dbs++;
		}
		else
		{
			pRow = &component_databases->entry_db_info[component_databases->total_entry_dbs];
			component_databases->total_entry_dbs++;
		}

		pRow->cdbs = component_databases;
		pRow->config = config;
		pRow->freelist = NIL;
		pRow->numIdleQEs = 0;
		pRow->numActiveQEs = 0;

		if (config->role != GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY || config->hostip == NULL)
			continue;

		hsEntry = (HostSegsEntry *) hash_search(hostSegsHash, config->hostip, HASH_ENTER, &found);
		if (found)
			hsEntry->segmentCount++;
		else
			hsEntry->segmentCount = 1;
	}

	/*
	 * Validate that there exists at least one entry and one segment database
	 * in the configuration
	 */
	if (component_databases->total_segment_dbs == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CARDINALITY_VIOLATION),
				 errmsg("number of segment databases cannot be 0")));
	}
	if (component_databases->total_entry_dbs == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CARDINALITY_VIOLATION),
				 errmsg("number of entry databases cannot be 0")));
	}

	/*
	 * Now sort the data by segindex, isprimary desc
	 */
	qsort(component_databases->segment_db_info,
		  component_databases->total_segment_dbs, sizeof(CdbComponentDatabaseInfo),
		  CdbComponentDatabaseInfoCompare);

	qsort(component_databases->entry_db_info,
		  component_databases->total_entry_dbs, sizeof(CdbComponentDatabaseInfo),
		  CdbComponentDatabaseInfoCompare);

	/*
	 * Now count the number of distinct segindexes. Since it's sorted, this is
	 * easy.
	 */
	for (i = 0; i < component_databases->total_segment_dbs; i++)
	{
		if (i == 0 ||
			(component_databases->segment_db_info[i].config->segindex != component_databases->segment_db_info[i - 1].config->segindex))
		{
			component_databases->total_segments++;
		}
	}

	/*
	 * Now validate that our identity is present in the entry databases
	 */
	for (i = 0; i < component_databases->total_entry_dbs; i++)
	{
		cdbInfo = &component_databases->entry_db_info[i];

		if (cdbInfo->config->dbid == GpIdentity.dbid && cdbInfo->config->segindex == GpIdentity.segindex)
		{
			break;
		}
	}
	if (i == component_databases->total_entry_dbs)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("cannot locate entry database"),
				 errdetail("entry database represented by this db in gp_segment_configuration: dbid %d content %d",
						   GpIdentity.dbid, GpIdentity.segindex)));
	}

	/*
	 * Now validate that the segindexes for the segment databases are between
	 * 0 and (numsegments - 1) inclusive, and that we hit them all.
	 * Since it's sorted, this is relatively easy.
	 */
	x = 0;
	for (i = 0; i < component_databases->total_segments; i++)
	{
		int			this_segindex = -1;

		while (x < component_databases->total_segment_dbs)
		{
			this_segindex = component_databases->segment_db_info[x].config->segindex;
			if (this_segindex < i)
				x++;
			else if (this_segindex == i)
				break;
			else if (this_segindex > i)
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATA_EXCEPTION),
						 errmsg("content values not valid in %s table",
								GpSegmentConfigRelationName),
						 errdetail("content values must be in the range 0 to %d inclusive.",
								   component_databases->total_segments - 1)));
			}
		}
		if (this_segindex != i)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("content values not valid in %s table",
							GpSegmentConfigRelationName),
					 errdetail("content values must be in the range 0 to %d inclusive",
							   component_databases->total_segments - 1)));
		}
	}

	for (i = 0; i < component_databases->total_segment_dbs; i++)
	{
		cdbInfo = &component_databases->segment_db_info[i];

		if (cdbInfo->config->role != GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY || cdbInfo->config->hostip == NULL)
			continue;

		hsEntry = (HostSegsEntry *) hash_search(hostSegsHash, cdbInfo->config->hostip, HASH_FIND, &found);
		Assert(found);
		cdbInfo->hostPrimaryCount = hsEntry->segmentCount;
	}

	for (i = 0; i < component_databases->total_entry_dbs; i++)
	{
		cdbInfo = &component_databases->entry_db_info[i];

		if (cdbInfo->config->role != GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY || cdbInfo->config->hostip == NULL)
			continue;

		hsEntry = (HostSegsEntry *) hash_search(hostSegsHash, cdbInfo->config->hostip, HASH_FIND, &found);
		Assert(found);
		cdbInfo->hostPrimaryCount = hsEntry->segmentCount;
	}

	hash_destroy(hostSegsHash);

	MemoryContextSwitchTo(oldContext);

	return component_databases;
}

/*
 * Helper function to clean up the idle segdbs list of
 * a segment component.
 */
static void
cleanupComponentIdleQEs(CdbComponentDatabaseInfo *cdi, bool includeWriter)
{
	SegmentDatabaseDescriptor	*segdbDesc;
	MemoryContext				oldContext;
	ListCell 					*curItem;

	Assert(CdbComponentsContext);
	oldContext = MemoryContextSwitchTo(CdbComponentsContext);
	curItem = list_head(cdi->freelist);

	foreach(curItem, cdi->freelist)
	{
		segdbDesc = (SegmentDatabaseDescriptor *)lfirst(curItem);
		Assert(segdbDesc);

		if (segdbDesc->isWriter && !includeWriter)
			continue;

		cdi->freelist = foreach_delete_current(cdi->freelist, curItem);
		DECR_COUNT(cdi, numIdleQEs);

		cdbconn_termSegmentDescriptor(segdbDesc);
	}

	MemoryContextSwitchTo(oldContext);
}

void
cdbcomponent_cleanupIdleQEs(bool includeWriter)
{
	CdbComponentDatabases	*cdbs;
	int						i;

	/* use cdb_component_dbs directly */
	cdbs = cdb_component_dbs;

	if (cdbs == NULL)		
		return;

	if (cdbs->segment_db_info != NULL)
	{
		for (i = 0; i < cdbs->total_segment_dbs; i++)
		{
			CdbComponentDatabaseInfo *cdi = &cdbs->segment_db_info[i];
			cleanupComponentIdleQEs(cdi, includeWriter);
		}
	}

	if (cdbs->entry_db_info != NULL)
	{
		for (i = 0; i < cdbs->total_entry_dbs; i++)
		{
			CdbComponentDatabaseInfo *cdi = &cdbs->entry_db_info[i];
			cleanupComponentIdleQEs(cdi, includeWriter);
		}
	}

	return;
}

/*
 * This function is called when a transaction is started and the snapshot of
 * segments info will not changed until the end of transaction
 */
void
cdbcomponent_updateCdbComponents(void)
{
	int expandVersion = GetGpExpandVersion();

	PG_TRY();
	{
		if (cdb_component_dbs == NULL)
		{
			cdb_component_dbs = getCdbComponentInfo();
			cdb_component_dbs->expand_version = GetGpExpandVersion();
		}
		else if (cdb_component_dbs->expand_version != expandVersion)
		{
			if (TempNamespaceOidIsValid())
			{
				/*
				 * Do not update here, otherwise, temp files will be lost
				 * in segments;
				 */
			}
			else
			{
				ELOG_DISPATCHER_DEBUG("FTS rescanned, get new component databases info.");
				cdbcomponent_destroyCdbComponents();
				cdb_component_dbs = getCdbComponentInfo();
				cdb_component_dbs->expand_version = expandVersion;
			}
		}
	}
	PG_CATCH();
	{
		FtsNotifyProber();

		PG_RE_THROW();
	}
	PG_END_TRY();

	Assert(cdb_component_dbs->numActiveQEs == 0);
}

/*
 * cdbcomponent_getCdbComponents
 *
 *
 * Storage for the SegmentInstances block and all subsidiary
 * structures are allocated from the caller's context.
 */
CdbComponentDatabases *
cdbcomponent_getCdbComponents()
{
	PG_TRY();
	{
		if (cdb_component_dbs == NULL)
		{
			cdb_component_dbs = getCdbComponentInfo();
			cdb_component_dbs->expand_version = GetGpExpandVersion();
		}
	}
	PG_CATCH();
	{
		if (Gp_role == GP_ROLE_DISPATCH)
			FtsNotifyProber();

		PG_RE_THROW();
	}
	PG_END_TRY();

	return cdb_component_dbs;
}

/*
 * cdbcomponet_destroyCdbComponents
 *
 * Disconnect and destroy all idle QEs, releases the memory
 * occupied by the CdbComponentDatabases
 *
 * callers must clean up QEs used by dispatcher states.
 */
void
cdbcomponent_destroyCdbComponents(void)
{
	/* caller must clean up all segdbs used by dispatcher states */
	Assert(!cdbcomponent_activeQEsExist());

	hash_destroy(segment_ip_cache_htab);
	segment_ip_cache_htab = NULL;

	/* disconnect and destroy idle QEs include writers */
	cdbcomponent_cleanupIdleQEs(true);

	/* delete the memory context */
	if (CdbComponentsContext)
		MemoryContextDelete(CdbComponentsContext);	
	CdbComponentsContext = NULL;
	cdb_component_dbs = NULL;
}

/*
 * Allocated a segdb
 *
 * If there is idle segdb in the freelist, return it, otherwise, initialize
 * a new segdb.
 *
 * idle segdbs has an established connection with segment, but new segdb is
 * not setup yet, callers need to establish the connection by themselves.
 */
SegmentDatabaseDescriptor *
cdbcomponent_allocateIdleQE(int contentId, SegmentType segmentType)
{
	SegmentDatabaseDescriptor	*segdbDesc = NULL;
	CdbComponentDatabaseInfo	*cdbinfo;
	ListCell 					*curItem;
	MemoryContext 				oldContext;
	bool						isWriter;

	cdbinfo = cdbcomponent_getComponentInfo(contentId);	

	oldContext = MemoryContextSwitchTo(CdbComponentsContext);

	/*
	 * Always try to pop from the head.  Make sure to push them back to head
	 * in cdbcomponent_recycleIdleQE().
	 */
	foreach(curItem, cdbinfo->freelist)
	{
		SegmentDatabaseDescriptor *tmp =
				(SegmentDatabaseDescriptor *)lfirst(curItem);
		Assert(tmp);

		if ((segmentType == SEGMENTTYPE_EXPLICT_WRITER && !tmp->isWriter) ||
			(segmentType == SEGMENTTYPE_EXPLICT_READER && tmp->isWriter))
			continue;

		cdbinfo->freelist = foreach_delete_current(cdbinfo->freelist, curItem);
		/* update numIdleQEs */
		DECR_COUNT(cdbinfo, numIdleQEs);

		segdbDesc = tmp;
		break;
	}

	if (!segdbDesc)
	{
		/*
		 * 1. for entrydb, it's never be writer.
		 * 2. for first QE, it must be a writer.
		 */
		isWriter = contentId == -1 ? false: (cdbinfo->numIdleQEs == 0 && cdbinfo->numActiveQEs == 0);
		segdbDesc = cdbconn_createSegmentDescriptor(cdbinfo, nextQEIdentifer(cdbinfo->cdbs), isWriter);
	}

	cdbconn_setQEIdentifier(segdbDesc, -1);

	INCR_COUNT(cdbinfo, numActiveQEs);

	MemoryContextSwitchTo(oldContext);

	return segdbDesc;
}

static bool
cleanupQE(SegmentDatabaseDescriptor *segdbDesc)
{
	Assert(segdbDesc != NULL);

#ifdef FAULT_INJECTOR
	if (SIMPLE_FAULT_INJECTOR("cleanup_qe") == FaultInjectorTypeSkip)
		return false;
#endif

	/*
	 * if the process is in the middle of blowing up... then we don't do
	 * anything here.  making libpq and other calls can definitely result in
	 * things getting HUNG.
	 */
	if (proc_exit_inprogress)
		return false;

	if (cdbconn_isBadConnection(segdbDesc))
		return false;

	/* if segment is down, the gang can not be reused */
	if (FtsIsSegmentDown(segdbDesc->segment_database_info))
		return false;

	/* If a reader exceed the cached memory limitation, destroy it */
	if (!segdbDesc->isWriter &&
		(segdbDesc->conn->mop_high_watermark >> 20) > gp_vmem_protect_gang_cache_limit)
		return false;

	/* Note, we cancel all "still running" queries */
	if (!cdbconn_discardResults(segdbDesc, 20))
	{
		elog(LOG, "cleaning up seg%d while it is still busy", segdbDesc->segindex);
		return false;
	}

	/* QE is no longer associated with a slice. */
	cdbconn_setQEIdentifier(segdbDesc, /* slice index */ -1);	

	return true;
}

void
cdbcomponent_recycleIdleQE(SegmentDatabaseDescriptor *segdbDesc, bool forceDestroy)
{
	CdbComponentDatabaseInfo	*cdbinfo;
	MemoryContext				oldContext;	
	int							maxLen;
	bool						isWriter;

	Assert(cdb_component_dbs);
	Assert(CdbComponentsContext);

	cdbinfo = segdbDesc->segment_database_info;
	isWriter = segdbDesc->isWriter;

	/* update num of active QEs */
	DECR_COUNT(cdbinfo, numActiveQEs);

	oldContext = MemoryContextSwitchTo(CdbComponentsContext);

	if (forceDestroy || !cleanupQE(segdbDesc))
		goto destroy_segdb;

	/* If freelist length exceed gp_cached_gang_threshold, destroy it */
	maxLen = segdbDesc->segindex == -1 ?
					MAX_CACHED_1_GANGS : gp_cached_gang_threshold;
	if (!isWriter && list_length(cdbinfo->freelist) >= maxLen)
		goto destroy_segdb;

	/* Recycle the QE, put it to freelist */
	if (isWriter)
	{
		/* writer is always the header of freelist */
		segdbDesc->segment_database_info->freelist =
			lcons(segdbDesc, segdbDesc->segment_database_info->freelist);
	}
	else
	{
		int			lastWriterPos = -1;
		ListCell   *cell;

		/*
		 * In cdbcomponent_allocateIdleQE() readers are always popped from the
		 * head, so to restore the original order we must pushed them back to
		 * the head, and keep in mind readers must be put after the writers.
		 */

		for (cell = list_head(segdbDesc->segment_database_info->freelist);
			 cell && ((SegmentDatabaseDescriptor *) lfirst(cell))->isWriter;
			 lastWriterPos++, cell = lnext(segdbDesc->segment_database_info->freelist, cell)) ;

		if (lastWriterPos >= 0)
			segdbDesc->segment_database_info->freelist = list_insert_nth(segdbDesc->segment_database_info->freelist,
							lastWriterPos + 1, segdbDesc);
		else
			segdbDesc->segment_database_info->freelist =
				lcons(segdbDesc, segdbDesc->segment_database_info->freelist);
	}

	INCR_COUNT(cdbinfo, numIdleQEs);

	MemoryContextSwitchTo(oldContext);

	return;

destroy_segdb:

	cdbconn_termSegmentDescriptor(segdbDesc);

	if (isWriter)
	{
		markCurrentGxactWriterGangLost();
	}

	MemoryContextSwitchTo(oldContext);
}

static int
nextQEIdentifer(CdbComponentDatabases *cdbs)
{
	int result;

	if (!cdbs->freeCounterList)
		return cdbs->qeCounter++;

	result = linitial_int(cdbs->freeCounterList);
	cdbs->freeCounterList = list_delete_first(cdbs->freeCounterList);
	return result;
}

bool
cdbcomponent_qesExist(void)
{
	return !cdb_component_dbs ? false :
			(cdb_component_dbs->numIdleQEs > 0 || cdb_component_dbs->numActiveQEs > 0);
}

bool
cdbcomponent_activeQEsExist(void)
{
	return !cdb_component_dbs ? false : cdb_component_dbs->numActiveQEs > 0;
}

/*
 * Find CdbComponentDatabaseInfo in the array by segment index.
 */
CdbComponentDatabaseInfo *
cdbcomponent_getComponentInfo(int contentId)
{
	CdbComponentDatabaseInfo *cdbInfo = NULL;
	CdbComponentDatabases *cdbs;

	cdbs = cdbcomponent_getCdbComponents();

	if (contentId < -1 || contentId >= cdbs->total_segments)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("unexpected content id %d, should be [-1, %d]",
						contentId, cdbs->total_segments - 1)));
	/* entry db */
	if (contentId == -1)
	{
		cdbInfo = &cdbs->entry_db_info[0];	
		return cdbInfo;
	}

	/* no mirror, segment_db_info is sorted by content id */
	if (cdbs->total_segment_dbs == cdbs->total_segments)
	{
		cdbInfo = &cdbs->segment_db_info[contentId];
		return cdbInfo;
	}

	/* with mirror, segment_db_info is sorted by content id */
	if (cdbs->total_segment_dbs != cdbs->total_segments)
	{
		Assert(cdbs->total_segment_dbs == cdbs->total_segments * 2);
		cdbInfo = &cdbs->segment_db_info[2 * contentId];

		if (!SEGMENT_IS_ACTIVE_PRIMARY(cdbInfo))
		{
			cdbInfo = &cdbs->segment_db_info[2 * contentId + 1];
		}

		return cdbInfo;
	}

	return cdbInfo;
}

static void
ensureInterconnectAddress(void)
{
	if (interconnect_address)
		return;

	if (GpIdentity.segindex >= 0)
	{
		Assert(Gp_role == GP_ROLE_EXECUTE);
		Assert(MyProcPort != NULL);
		Assert(MyProcPort->laddr.addr.ss_family == AF_INET
				|| MyProcPort->laddr.addr.ss_family == AF_INET6);
		/*
		 * We assume that the QD, using the address in gp_segment_configuration
		 * as its destination IP address, connects to the segment/QE.
		 * So, the local address in the PORT can be used for interconnect.
		 */
		char local_addr[NI_MAXHOST];
		getnameinfo((const struct sockaddr *)&MyProcPort->laddr.addr,
					MyProcPort->laddr.salen,
					local_addr, sizeof(local_addr),
					NULL, 0, NI_NUMERICHOST);
		interconnect_address = MemoryContextStrdup(TopMemoryContext, local_addr);
	}
	else if (Gp_role == GP_ROLE_DISPATCH)
	{
		/*
		 * Here, we can only retrieve the ADDRESS in gp_segment_configuration
		 * from `cdbcomponent*`. We couldn't get it in a way as the QEs.
		 */
		CdbComponentDatabaseInfo *qdInfo;
		qdInfo = cdbcomponent_getComponentInfo(MASTER_CONTENT_ID);
		interconnect_address = MemoryContextStrdup(TopMemoryContext, qdInfo->config->hostip);
	}
	else if (qdHostname && qdHostname[0] != '\0')
	{
		Assert(Gp_role == GP_ROLE_EXECUTE);
		/*
		 * QE on the master can't get its interconnect address like that on the primary.
		 * The QD connects to its postmaster via the unix domain socket.
		 */
		interconnect_address = qdHostname;
	}
	else
		Assert(false);
}
/*
 * performs all necessary setup required for Apache Cloudberry mode.
 *
 * This includes cdblink_setup() and initializing the Motion Layer.
 */
void
cdb_setup(void)
{
	elog(DEBUG1, "Initializing Cloudberry components...");

	if (Gp_role != GP_ROLE_UTILITY && !IS_SINGLENODE())
	{
		if (!CurrentMotionIPCLayer) {
			ereport(ERROR,
				(errmsg("Interconnect moudle have not been preloaded"),
				 errdetail("Please make sure interconnect is included in option shared_preload_libraries")));
		}
		ensureInterconnectAddress();
		/* Initialize the Motion Layer IPC subsystem. */
		CurrentMotionIPCLayer->InitMotionLayerIPC();
	}

	/*
	 * Backend process requires consistent state, it cannot proceed until
	 * dtx recovery process finish up the recovery of distributed transactions.
	 *
	 * Ignore background worker because bgworker_should_start_mpp() already did
	 * the check.
	 */
	if (!IsBackgroundWorker &&
		Gp_role == GP_ROLE_DISPATCH &&
		!*shmDtmStarted)
	{
		ereport(FATAL,
				(errcode(ERRCODE_CANNOT_CONNECT_NOW),
				 errmsg(POSTMASTER_IN_RECOVERY_MSG),
				 errdetail("waiting for distributed transaction recovery to complete")));
	}
}

/*
 * performs all necessary cleanup required when leaving Cloudberry
 * Database mode.  This is also called when the process exits.
 *
 * NOTE: the arguments to this function are here only so that we can
 *		 register it with on_proc_exit().  These parameters should not
 *		 be used since there are some callers to this that pass them
 *		 as NULL.
 *
 */
void
cdb_cleanup(int code pg_attribute_unused(), Datum arg
						pg_attribute_unused())
{
	elog(DEBUG1, "Cleaning up Cloudberry components...");

	DisconnectAndDestroyAllGangs(true);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		if (cdb_total_plans > 0)
		{
			elog(DEBUG1, "session dispatched %d plans %d slices (%f), largest plan %d",
				 cdb_total_plans, cdb_total_slices,
				 ((double) cdb_total_slices / (double) cdb_total_plans),
				 cdb_max_slices);
		}
	}

	if (Gp_role != GP_ROLE_UTILITY && !IS_SINGLENODE())
	{
		/* shutdown our listener socket */
		CurrentMotionIPCLayer->CleanUpMotionLayerIPC();
	}
}

/*
 * CdbComponentDatabaseInfoCompare:
 * A compare function for CdbComponentDatabaseInfo structs
 * that compares based on , isprimary desc
 * for use with qsort.
 */
static int
CdbComponentDatabaseInfoCompare(const void *p1, const void *p2)
{
	const CdbComponentDatabaseInfo *obj1 = (CdbComponentDatabaseInfo *) p1;
	const CdbComponentDatabaseInfo *obj2 = (CdbComponentDatabaseInfo *) p2;

	int			cmp = obj1->config->segindex - obj2->config->segindex;

	if (cmp == 0)
	{
		int			obj2cmp = 0;
		int			obj1cmp = 0;

		if (SEGMENT_IS_ACTIVE_PRIMARY(obj2))
			obj2cmp = 1;

		if (SEGMENT_IS_ACTIVE_PRIMARY(obj1))
			obj1cmp = 1;

		cmp = obj2cmp - obj1cmp;
	}

	return cmp;
}

/*
 * Maintain a cache of names.
 *
 * The keys are all NAMEDATALEN long.
 */
static char *
getDnsCachedAddress(char *name, int port, int elevel, bool use_cache)
{
	SegIpEntry	   *e = NULL;
	char			hostinfo[NI_MAXHOST];

	if (use_cache)
	{
		if (segment_ip_cache_htab == NULL)
		{
			HASHCTL		hash_ctl;

			MemSet(&hash_ctl, 0, sizeof(hash_ctl));

			hash_ctl.keysize = NAMEDATALEN + 1;
			hash_ctl.entrysize = sizeof(SegIpEntry);

			segment_ip_cache_htab = hash_create("segment_dns_cache",
												256, &hash_ctl, HASH_ELEM | HASH_STRINGS);
		}
		else
		{
			e = (SegIpEntry *) hash_search(segment_ip_cache_htab,
										   name, HASH_FIND, NULL);
			if (e != NULL)
				return e->hostinfo;
		}
	}

	/*
	 * The name is either not in our cache, or we've been instructed to not
	 * use the cache. Perform the name lookup.
	 */
	if (!use_cache || (use_cache && e == NULL))
	{
		MemoryContext oldContext = NULL;
		int			ret;
		char		portNumberStr[32];
		char	   *service;
		struct addrinfo *addrs = NULL,
				   *addr;
		struct addrinfo hint;

		/* Initialize hint structure */
		MemSet(&hint, 0, sizeof(hint));
		hint.ai_socktype = SOCK_STREAM;
		hint.ai_family = AF_UNSPEC;

		snprintf(portNumberStr, sizeof(portNumberStr), "%d", port);
		service = portNumberStr;

		ret = pg_getaddrinfo_all(name, service, &hint, &addrs);
		if (ret || !addrs)
		{
			if (addrs)
				pg_freeaddrinfo_all(hint.ai_family, addrs);

			/*
			 * If a host name is unknown, whether it is an error depends on its role:
			 * - if it is a primary then it's an error;
			 * - if it is a mirror then it's just a warning;
			 * but we do not know the role information here, so always treat it as a
			 * warning, the callers should check the role and decide what to do.
			 */
			if (ret != EAI_FAIL && elevel == ERROR)
				elevel = WARNING;

			ereport(elevel,
					(errmsg("could not translate host name \"%s\", port \"%d\" to address: %s",
							name, port, gai_strerror(ret))));

			return NULL;
		}

		/* save in the cache context */
		if (use_cache)
			oldContext = MemoryContextSwitchTo(TopMemoryContext);

		hostinfo[0] = '\0';
		for (addr = addrs; addr; addr = addr->ai_next)
		{
#ifdef HAVE_UNIX_SOCKETS
			/* Ignore AF_UNIX sockets, if any are returned. */
			if (addr->ai_family == AF_UNIX)
				continue;
#endif
			if (addr->ai_family == AF_INET) /* IPv4 address */
			{
				memset(hostinfo, 0, sizeof(hostinfo));
				pg_getnameinfo_all((struct sockaddr_storage *) addr->ai_addr, addr->ai_addrlen,
								   hostinfo, sizeof(hostinfo),
								   NULL, 0,
								   NI_NUMERICHOST);

				if (use_cache)
				{
					/* Insert into our cache htab */
					e = (SegIpEntry *) hash_search(segment_ip_cache_htab,
												   name, HASH_ENTER, NULL);
					memcpy(e->hostinfo, hostinfo, sizeof(hostinfo));
				}

				break;
			}
		}

#ifdef HAVE_IPV6

		/*
		 * IPv6 probably would work fine, we'd just need to make sure all the
		 * data structures are big enough for the IPv6 address.  And on some
		 * broken systems, you can get an IPv6 address, but not be able to
		 * bind to it because IPv6 is disabled or missing in the kernel, so
		 * we'd only want to use the IPv6 address if there isn't an IPv4
		 * address.  All we really need to do is test this.
		 */
		if (((!use_cache && !hostinfo[0]) || (use_cache && e == NULL))
			&& addrs->ai_family == AF_INET6)
		{
			addr = addrs;
			/* Get a text representation of the IP address */
			pg_getnameinfo_all((struct sockaddr_storage *) addr->ai_addr, addr->ai_addrlen,
							   hostinfo, sizeof(hostinfo),
							   NULL, 0,
							   NI_NUMERICHOST);

			if (use_cache)
			{
				/* Insert into our cache htab */
				e = (SegIpEntry *) hash_search(segment_ip_cache_htab,
											   name, HASH_ENTER, NULL);
				memcpy(e->hostinfo, hostinfo, sizeof(hostinfo));
			}
		}
#endif

		if (use_cache)
			MemoryContextSwitchTo(oldContext);

		pg_freeaddrinfo_all(hint.ai_family, addrs);
	}

	/* return a pointer to our cache. */
	if (use_cache)
		return e->hostinfo;

	return pstrdup(hostinfo);
}

/*
 * getDnsAddress
 *
 * same as getDnsCachedAddress, but without using the cache. A non-cached
 * version was used inline inside of cdbgang.c, and since it is needed now
 * elsewhere, it is available externally.
 */
char *
getDnsAddress(char *hostname, int port, int elevel)
{
	return getDnsCachedAddress(hostname, port, elevel, false);
}


/*
 * Given a component-db in the system, find the addresses at which it
 * can be reached, appropriately populate the argument-structure, and
 * maintain the ip-lookup-cache.
 */
static void
getAddressesForDBid(GpSegConfigEntry *c, int elevel)
{
	char	   *name;

	Assert(c != NULL);

	/* Use hostname */
	memset(c->hostaddrs, 0, COMPONENT_DBS_MAX_ADDRS * sizeof(char *));

#ifdef FAULT_INJECTOR
	if (SIMPLE_FAULT_INJECTOR("get_dns_cached_address") == FaultInjectorTypeSkip)
	{
		/* inject a dns error for primary of segment 0 */
		if (c->segindex == 0 &&
				c->preferred_role == GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY)
		{
			c->address = pstrdup("dnserrordummyaddress");
			c->hostname = pstrdup("dnserrordummyaddress");
		}
	}
#endif

	/*
	 * add an entry, using the first the "address" and then the "hostname" as
	 * fallback.
	 */
	name = getDnsCachedAddress(c->address, c->port, elevel, true);

	if (name)
	{
		c->hostaddrs[0] = pstrdup(name);
		return;
	}

	/* now the hostname. */
	name = getDnsCachedAddress(c->hostname, c->port, elevel, true);
	if (name)
	{
		c->hostaddrs[0] = pstrdup(name);
	}
	else
	{
		c->hostaddrs[0] = NULL;
	}

	return;
}

/*
 * hostSegsHashTableInit()
 *    Construct a hash table of HostSegsEntry
 */
static HTAB *
hostSegsHashTableInit(void)
{
	HASHCTL		info;

	/* Set key and entry sizes. */
	MemSet(&info, 0, sizeof(info));
	info.keysize = INET6_ADDRSTRLEN;
	info.entrysize = sizeof(HostSegsEntry);

	return hash_create("HostSegs", 32, &info, HASH_ELEM | HASH_STRINGS);
}

/*
 * Given total number of primary segment databases and a number of
 * segments to "skip" - this routine creates a boolean map (array) the
 * size of total number of segments and randomly selects several
 * entries (total number of total_to_skip) to be marked as
 * "skipped". This is used for external tables with the 'gpfdist'
 * protocol where we want to get a number of *random* segdbs to
 * connect to a gpfdist client.
 *
 * Caller of this function should pfree skip_map when done with it.
 */
bool *
makeRandomSegMap(int total_primaries, int total_to_skip)
{
	int			randint;		/* some random int representing a seg    */
	int			skipped = 0;	/* num segs already marked to be skipped */
	bool	   *skip_map;

	skip_map = (bool *) palloc(total_primaries * sizeof(bool));
	MemSet(skip_map, false, total_primaries * sizeof(bool));

	while (total_to_skip != skipped)
	{
		/*
		 * create a random int between 0 and (total_primaries - 1).
		 */
		randint = cdbhashrandomseg(total_primaries);

		/*
		 * mark this random index 'true' in the skip map (marked to be
		 * skipped) unless it was already marked.
		 */
		if (skip_map[randint] == false)
		{
			skip_map[randint] = true;
			skipped++;
		}
	}

	return skip_map;
}

/*
 * Determine the dbid for the master standby
 */
int16
master_standby_dbid(void)
{
	int total_dbs;
	GpSegConfigEntry *configs = NULL;
	GpSegConfigEntry *config = NULL;
	int16 dbid = 0;

	/*
	 * Can only run on a master node, this restriction is due to the reliance
	 * on the gp_segment_configuration table.
	 */
	if (!IS_QUERY_DISPATCHER())
		elog(ERROR, "master_standby_dbid() executed on execution segment");

	configs = readGpSegConfigFromETCD(&total_dbs, false);

	/*
	 * SELECT dbid FROM gp_segment_configuration WHERE content = -1 AND role =
	 * GP_SEGMENT_CONFIGURATION_ROLE_MIRROR
	 */
	for (int i = 0; i < total_dbs; i++)
	{
		config = &configs[i];
		if (config->role == GP_SEGMENT_CONFIGURATION_ROLE_MIRROR && config->segindex == -1)
		{
			dbid = config->dbid;
			break;
		}
	}

	cleanGpSegConfigs(configs, total_dbs);
	return dbid;
}

GpSegConfigEntry *
dbid_get_dbinfo(int16 dbid)
{
	int total_dbs;
	GpSegConfigEntry *configs = NULL;
	GpSegConfigEntry *config = NULL;
	GpSegConfigEntry *config_rc = NULL;

	/* Should not call this method in TopMemoryContext
	 * It will make memory leak
	 */ 
	Assert(CurrentMemoryContext != TopMemoryContext);

	/*
	 * Can only run on a master node, this restriction is due to the reliance
	 * on the gp_segment_configuration table.  This may be able to be relaxed
	 * by switching to a different method of checking.
	 */
	if (!IS_QUERY_DISPATCHER())
		elog(ERROR, "dbid_get_dbinfo() executed on execution segment");

	configs = readGpSegConfigFromETCD(&total_dbs, false);
	for (int i = 0; i < total_dbs; i++)
	{
		config = &configs[i];
		if (config->dbid == dbid)
		{
			config_rc = palloc0(sizeof(GpSegConfigEntry));
			copyGpsegConfig(config, config_rc);
			break;
		}
	}

	cleanGpSegConfigs(configs, total_dbs);
	return config_rc;
}

/*
 * Obtain the dbid of a of a segment at a given segment index (i.e., content id)
 * currently fulfilling the role specified. This means that the segment is
 * really performing the role of primary or mirror, irrespective of their
 * preferred role.
 */
int16
contentid_get_dbid(int16 contentid, char role, bool getPreferredRoleNotCurrentRole)
{
	int16		dbid = 0;
	int total_dbs;
	GpSegConfigEntry *configs = NULL;
	GpSegConfigEntry *config = NULL;

	/*
	 * Can only run on a master node, this restriction is due to the reliance
	 * on the gp_segment_configuration table.  This may be able to be relaxed
	 * by switching to a different method of checking.
	 */
	if (!IS_QUERY_DISPATCHER())
		elog(ERROR, "contentid_get_dbid() executed on execution segment");

	configs = readGpSegConfigFromETCD(&total_dbs, false);

	for (int i = 0; i < total_dbs; i++)
	{
		config = &configs[i];
		if (config->segindex == contentid && 
			(getPreferredRoleNotCurrentRole ? config->preferred_role == role : config->role == role))
		{
			dbid = config->dbid;
			break;
		}
	}

	cleanGpSegConfigs(configs, total_dbs);
	return dbid;
}

int16 
cdbcomponent_get_maxdbid(void)
{
	int16		dbid = 0;
	int total_dbs;
	GpSegConfigEntry *configs = NULL;
	GpSegConfigEntry *config = NULL;

	configs = readGpSegConfigFromETCD(&total_dbs, false);

	for (int i = 0; i < total_dbs; i++)
	{
		config = &configs[i];
		dbid = Max(dbid, config->dbid);
	}

	cleanGpSegConfigs(configs, total_dbs);
	return dbid;
}


int16
cdbcomponent_get_availableDbId(void)
{
	HASHCTL		hash_ctl;
	
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(int32);
	hash_ctl.entrysize = sizeof(int32);
	hash_ctl.hash = int32_hash;
	HTAB	   *htab = hash_create("Temporary table of dbids",
								   1024,
								   &hash_ctl,
								   HASH_ELEM | HASH_FUNCTION);

	/* scan gp_segment_configuration in ETCD */
	int total_dbs;
	GpSegConfigEntry *configs = NULL;
	GpSegConfigEntry *config = NULL;

	/*
	 * Set up hash of used dbids.  We use int32 here because int16 doesn't
	 * have a convenient hash and we can use casting below to check for
	 * overflow of int16
	 */
	int32 dbid = 0;

	configs = readGpSegConfigFromETCD(&total_dbs, false);

	for (int i = 0; i < total_dbs; i++)
	{
		config = &configs[i];
		dbid = config->dbid;
		(void) hash_search(htab, (void *) &dbid, HASH_ENTER, NULL);
	}

	/* search for available dbid */
	for (dbid = 1;; dbid++)
	{
		if (dbid != (int16) dbid)
			elog(ERROR, "unable to find available dbid");

		if (hash_search(htab, (void *) &dbid, HASH_FIND, NULL) == NULL)
		{
			hash_destroy(htab);
			cleanGpSegConfigs(configs, total_dbs);
			return (int16) dbid;
		}
	}
	cleanGpSegConfigs(configs, total_dbs);
}

int16
cdbcomponent_get_maxcontentid(void)
{
	int16		segindex = 0;
	int total_dbs;
	GpSegConfigEntry *configs = NULL;
	GpSegConfigEntry *config = NULL;

	configs = readGpSegConfigFromETCD(&total_dbs, false);

	for (int i = 0; i < total_dbs; i++)
	{
		config = &configs[i];
		segindex = Max(segindex, config->segindex);
	}

	cleanGpSegConfigs(configs, total_dbs);
	return segindex;
}

List *
cdbcomponent_getCdbComponentsList(void)
{
	CdbComponentDatabases *cdbs;
	List *segments = NIL;
	int i;

	cdbs = cdbcomponent_getCdbComponents();

	for (i = 0; i < cdbs->total_segments; i++)
	{
		segments = lappend_int(segments, i);
	}

	return segments;
}

/*
 * return the number of total segments for current snapshot of
 * segments info
 */
int
getgpsegmentCount(void)
{
	/* 1 represents a singleton postgresql in utility mode */
	int32 numsegments = 1;

	if (Gp_role == GP_ROLE_DISPATCH)
		numsegments = cdbcomponent_getCdbComponents()->total_segments;
	else if (Gp_role == GP_ROLE_EXECUTE)
		numsegments = numsegmentsFromQD;
	/*
	 * If we are in 'Utility & Binary Upgrade' mode, it must be launched
	 * by the pg_upgrade, so we give it an correct numsegments to make
	 * sure the pg_upgrade can run normally.
	 * Only Utility QD process have the entire information in the
	 * gp_segment_configuration, so we count the segments count in this
	 * process.
	 */
	else if (Gp_role == GP_ROLE_UTILITY &&
			 IsBinaryUpgrade &&
			 IS_QUERY_DISPATCHER())
	{
		numsegments = cdbcomponent_getCdbComponents()->total_segments;
	}

	return numsegments;
}

/*
 * IsOnConflictUpdate
 * Return true if a plannedstmt is an upsert: insert ... on conflict do update
 */
bool
IsOnConflictUpdate(PlannedStmt *ps)
{
	Plan      *plan;

	if (ps == NULL || ps->commandType != CMD_INSERT)
		return false;

	plan = ps->planTree;

	if (plan && IsA(plan, Motion))
		plan = outerPlan(plan);

	if (plan == NULL || !IsA(plan, ModifyTable))
		return false;

	return ((ModifyTable *)plan)->onConflictAction == ONCONFLICT_UPDATE;
}

/*
 * Avoid core file generation for this PANIC. It helps to avoid
 * filling up disks during tests and also saves time.
 */
void
AvoidCorefileGeneration()
{
#if defined(HAVE_GETRLIMIT) && defined(RLIMIT_CORE)
	struct rlimit lim;
	getrlimit(RLIMIT_CORE, &lim);
	lim.rlim_cur = 0;
	if (setrlimit(RLIMIT_CORE, &lim) != 0)
	{
		int			save_errno = errno;

		elog(NOTICE,
			 "setrlimit failed for RLIMIT_CORE soft limit to zero. errno: %d (%m).",
			 save_errno);
	}
#endif
}

PG_FUNCTION_INFO_V1(gp_get_suboverflowed_backends);
/*
 * Find the backends where subtransaction overflowed.
 */
Datum
gp_get_suboverflowed_backends(PG_FUNCTION_ARGS)
{
	int 			i;
	ArrayBuildState *astate = NULL;

	LWLockAcquire(ProcArrayLock, LW_SHARED);
	for (i = 0; i < ProcGlobal->allProcCount; i++)
	{
		if (ProcGlobal->subxidStates[i].overflowed)
			astate = accumArrayResult(astate,
									  Int32GetDatum(ProcGlobal->allProcs[i].pid),
									  false, INT4OID, CurrentMemoryContext);
	}
	LWLockRelease(ProcArrayLock);

	if (astate)
		PG_RETURN_DATUM(makeArrayResult(astate,
											CurrentMemoryContext));
	else
		PG_RETURN_NULL();
}

#endif /* USE_INTERNAL_FTS */
