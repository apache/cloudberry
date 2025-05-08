#undef GP_VERSION
#include "postgres_fe.h"

#include "apr_general.h"
#include "apr_hash.h"
#include "apr_time.h"
#include "apr_queue.h"
#include "apr_strings.h"
#include "gpmon.h"
#include "gpmondb.h"
#include "gpmon_agg.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <math.h>
#include <sys/stat.h>

typedef enum disk_space_message_t
{
	DISK_SPACE_NO_MESSAGE_SENT = 0,
	DISK_SPACE_WARNING_SENT ,
	DISK_SPACE_ERROR_SENT
} disk_space_message_t;

typedef struct mmon_fsinfo_t
{
	gpmon_fsinfokey_t key;

	apr_int64_t				bytes_used;
	apr_int64_t				bytes_available;
	apr_int64_t				bytes_total;
	disk_space_message_t	sent_error_flag;
	time_t					last_update_timestamp;
} mmon_fsinfo_t; //the fsinfo structure used in mmon

typedef struct mmon_qexec_t
{
	gpmon_qexeckey_t 	key;
	apr_uint64_t 		rowsout;
	apr_uint64_t		_cpu_elapsed; /* CPU elapsed for iter */
	apr_uint64_t 		measures_rows_in;
} mmon_qexec_t;  //The qexec structure used in mmon

typedef struct mmon_query_seginfo_t
{
	gpmon_query_seginfo_key_t	key;
	apr_uint64_t				sum_cpu_elapsed;
} mmon_query_seginfo_t;  //The agg value at segment level for query

struct agg_t
{
	apr_int64_t generation;
	apr_pool_t* pool;
	apr_pool_t* parent_pool;
	apr_hash_t* qtab;		/* key = gpmon_qlog_key_t, value = qdnode ptr. */
	apr_hash_t* htab;		/* key = hostname, value = gpmon_metrics_t ptr */
	apr_hash_t* stab;		/* key = databaseid, value = gpmon_seginfo_t ptr */
	apr_hash_t* fsinfotab;	/* This is the persistent fsinfo hash table: key = gpmon_fsinfokey_t, value = mmon_fsinfo_t ptr */
};

typedef struct dbmetrics_t {
	apr_int32_t queries_total;
	apr_int32_t queries_running;
	apr_int32_t queries_queued;
} dbmetrics_t;

extern int min_query_time;
extern mmon_options_t opt;
extern apr_queue_t* message_queue;
extern int32 tmid;

extern void incremement_tail_bytes(apr_uint64_t bytes);
static bool is_query_not_active(gpmon_qlogkey_t qkey, apr_hash_t *hash, apr_pool_t *pool);
static void format_time(time_t tt, char *buf);
static void set_tmid(gp_smon_to_mmon_packet_t* pkt, int32 tmid);
static void update_query_now_metrics(qdnode_t* qdnode, long *spill_file_size);
static void update_query_history_metrics(qdnode_t* qdnode);
static bool is_query_not_active(gpmon_qlogkey_t qkey, apr_hash_t *hash, apr_pool_t *pool)
{
	// get active query of session
	char *key = apr_psprintf(pool, "%d", qkey.ssid);
	char *active_query = apr_hash_get(hash, key, APR_HASH_KEY_STRING);
	if (active_query == NULL)
	{
		TR0(("Found orphan query, tmid:%d, ssid:%d, ccnt:%d\n", qkey.tmid, qkey.ssid, qkey.ccnt));
		return true;
	}

	// read query text from q file
	char *query = get_query_text(qkey, pool);
	if (query == NULL)
	{
		TR0(("Found error while reading query text in file '%sq%d-%d-%d.txt'\n", GPMON_DIR, qkey.tmid, qkey.ssid, qkey.ccnt));
		return true;
	}
	// if the current active query of session (ssid) is not the same
	// as the one we are checking, we assume q(tmid)-(ssid)-(ccnt).txt
	// has wrong status. This is a bug in execMain.c, which too hard to
	// fix it there.
	int qlen = strlen(active_query);
	if (qlen > MAX_QUERY_COMPARE_LENGTH)
	{
		qlen = MAX_QUERY_COMPARE_LENGTH;
	}
	int res = strncmp(query, active_query, qlen);
	if (res != 0)
	{
		TR0(("Found orphan query, tmid:%d, ssid:%d, ccnt:%d\n", qkey.tmid, qkey.ssid, qkey.ccnt));
		return true;
	}

	return false;
}

static apr_status_t agg_put_fsinfo(agg_t* agg, const gpmon_fsinfo_t* met)
{
	mmon_fsinfo_t* rec;

	rec = apr_hash_get(agg->fsinfotab, &met->key, sizeof(met->key));
	if (!rec) {
		// Use the parent pool because we need the fsinfo to be persistent and never be freed
		rec = apr_palloc(agg->parent_pool, sizeof(*rec));
		if (!rec)
			return APR_ENOMEM;
		rec->key = met->key;
		rec->sent_error_flag = DISK_SPACE_NO_MESSAGE_SENT;
		apr_hash_set(agg->fsinfotab, &met->key, sizeof(met->key), rec);
	}
	rec->bytes_available = met->bytes_available;
	rec->bytes_total = met->bytes_total;
	rec->bytes_used = met->bytes_used;
	rec->last_update_timestamp = time(NULL); //set the updated timestamp for the packet

	// if both the option percentages are set to 0 than the disk space check is disabled
	// Also if max_disk_space_messages_per_interval is 0 the disk space check is disabled
	//if (((opt.warning_disk_space_percentage) || (opt.error_disk_space_percentage)) &&
	//		(opt.max_disk_space_messages_per_interval != 0)) {
	//	check_disk_space(rec);
	//}
	return 0;
}

static apr_status_t agg_put_queryseg(agg_t* agg, const gpmon_query_seginfo_t* met, apr_int64_t generation)
{
	qdnode_t* dp;
	gpmon_qlogkey_t key = met->key.qkey;
	mmon_query_seginfo_t* rec = 0;

	/* find qdnode of this qexec */
	dp = apr_hash_get(agg->qtab, &key, sizeof(key));

	if (!dp) { /* not found, internal SPI query.  Ignore. */
		return 0;
	}
	rec = apr_hash_get(dp->query_seginfo_hash, &met->key.segid, sizeof(met->key.segid));

	/* if found, replace it */
	if (rec) {
		rec->sum_cpu_elapsed = met->sum_cpu_elapsed;
	}
	else {
		/* not found, make new hash entry */

		if (!(rec = apr_palloc(agg->pool, sizeof(mmon_query_seginfo_t)))){

			return APR_ENOMEM;
		}
		memcpy(&rec->key, &met->key, sizeof(gpmon_query_seginfo_key_t));
		rec->sum_cpu_elapsed = met->sum_cpu_elapsed;

		apr_hash_set(dp->query_seginfo_hash, &rec->key.segid, sizeof(rec->key.segid), rec);
	}

	dp->last_updated_generation = generation;
	return 0;
}

static apr_status_t agg_put_metrics(agg_t* agg, const gpmon_metrics_t* met)
{
	gpmon_metrics_t* rec;

	rec = apr_hash_get(agg->htab, met->hname, APR_HASH_KEY_STRING);
	if (rec) {
		*rec = *met;
	} else {
		rec = apr_palloc(agg->pool, sizeof(*rec));
		if (!rec)
			return APR_ENOMEM;
		*rec = *met;
		apr_hash_set(agg->htab, rec->hname, APR_HASH_KEY_STRING, rec);
	}
	return 0;
}

static apr_status_t agg_put_query_metrics(agg_t* agg, const gpmon_qlog_t* qlog, apr_int64_t generation, char* host_ip)
{
	qdnode_t *node;

	node = apr_hash_get(agg->qtab, &qlog->key, sizeof(qlog->key));
        int* exist;
	if (!node)
	{
		TR2(("put query metrics can not find qdnode from qtab, queryID :%d-%d-%d, Host Ip:%s \n",
			 qlog->key.tmid, qlog->key.ssid, qlog->key.ccnt, host_ip));
	}
	if (node)
	{
                exist = apr_hash_get(node->host_hash, host_ip, strlen(host_ip));
                if(!exist)
                {
                        exist = apr_pcalloc(agg->pool, sizeof(int));
                        *exist = 1;
                        apr_hash_set(node->host_hash, host_ip, strlen(host_ip), exist);
                        node->host_cnt++;
                }
                else
                {
                        ASSERT(*exist == 1);
                }

                // It is used to calculate the real-time value of the metrics for a small time period of the query.
                node->p_interval_metrics.cpu_pct += qlog->p_metrics.cpu_pct;
                node->p_interval_metrics.mem.resident += qlog->p_metrics.mem.resident;
                node->num_metrics_packets_interval++;

		node->last_updated_generation = generation;
		TR2(("Query Metrics, Query ID: %d-%d-%d , Host Ip:%s (cpu_pct %f mem_resident %lu), interval pkt:%d, host cnt:%d\n",
			 qlog->key.tmid, qlog->key.ssid, qlog->key.ccnt, host_ip, qlog->p_metrics.cpu_pct, qlog->p_metrics.mem.resident,
			node->num_metrics_packets_interval, node->host_cnt));
	}
	return 0;
}

static apr_status_t agg_put_qlog(agg_t* agg, const gpmon_qlog_t* qlog,
				 apr_int64_t generation)
{
        if (qlog->dbid == gpperfmon_dbid) {
                TR2(("agg_put_qlog:(%d-%d-%d) ignore gpperfmon sql\n", qlog->key.tmid, qlog->key.ssid, qlog->key.ccnt));
                return 0;
        }

        qdnode_t* node;
	node = apr_hash_get(agg->qtab, &qlog->key, sizeof(qlog->key));
	if (node) {
		node->qlog.status = qlog->status;
		node->qlog.tstart = qlog->tstart;
		node->qlog.tsubmit = qlog->tsubmit;
		node->qlog.tfin = qlog->tfin;
		if (qlog->dbid != gpperfmon_dbid) {
			TR2(("agg_put_qlog: found %d-%d-%d generation %d recorded %d\n", qlog->key.tmid, qlog->key.ssid, qlog->key.ccnt, (int) generation, node->recorded));
		}
	} else {
		node = apr_pcalloc(agg->pool, sizeof(*node));
		if (!node)
			return APR_ENOMEM;

		node->qlog = *qlog;
		node->recorded = 0;
		node->qlog.cpu_elapsed = 0;
                memset(&node->qlog.p_metrics, 0, sizeof(node->qlog.p_metrics));
                memset(&node->p_interval_metrics, 0, sizeof(node->p_interval_metrics));
                memset(&node->p_queries_history_metrics, 0, sizeof(node->p_queries_history_metrics));
                memset(&node->p_queries_now_metrics, 0, sizeof(node->p_queries_now_metrics));
                node->host_cnt = 0;
		node->num_cpu_pct_interval_total = 0;
                node->num_metrics_packets_interval = 0;

		node->host_hash = apr_hash_make(agg->pool);
		if (!node->host_hash) {
			TR2(("agg_put_qlog: host_hash = apr_hash_make(agg->pool) returned null\n"));
			return APR_ENOMEM;
		}

		node->query_seginfo_hash = apr_hash_make(agg->pool);
		if (!node->query_seginfo_hash) {
			TR2(("agg_put_qlog: query_seginfo_hash = apr_hash_make(agg->pool) returned null\n"));
			return APR_ENOMEM;
		}

		apr_hash_set(agg->qtab, &node->qlog.key, sizeof(node->qlog.key), node);
		if (qlog->dbid != gpperfmon_dbid) {
			TR2(("agg_put: new %d-%d-%d generation %d recorded %d\n", qlog->key.tmid, qlog->key.ssid, qlog->key.ccnt, (int) generation, node->recorded));
		}
	}
	node->last_updated_generation = generation;

	return 0;
}

apr_status_t agg_create(agg_t** retagg, apr_int64_t generation, apr_pool_t* parent_pool, apr_hash_t* fsinfotab)
{
	int e;
	apr_pool_t* pool;
	agg_t* agg;

	if (0 != (e = apr_pool_create_alloc(&pool, parent_pool)))
		return e;

	agg = apr_pcalloc(pool, sizeof(*agg));
	if (!agg) {
		apr_pool_destroy(pool);
		return APR_ENOMEM;
	}

	agg->generation = generation;
	agg->pool = pool;
	agg->parent_pool = parent_pool;
	agg->fsinfotab = fsinfotab; // This hash table for the fsinfo is persistent and will use the parent pool

	agg->qtab = apr_hash_make(pool);
	if (!agg->qtab) {
		apr_pool_destroy(pool);
		return APR_ENOMEM;
	}

	agg->htab = apr_hash_make(pool);
	if (!agg->htab) {
		apr_pool_destroy(pool);
		return APR_ENOMEM;
	}

	agg->stab = apr_hash_make(pool);
	if (!agg->stab) {
		apr_pool_destroy(pool);
		return APR_ENOMEM;
	}

	*retagg = agg;
	return 0;
}

apr_status_t agg_dup(agg_t** retagg, agg_t* oldagg, apr_pool_t* parent_pool, apr_hash_t* fsinfotab)
{
	int e, cnt;
	agg_t* newagg;
	apr_hash_index_t *hi, *hj;

	if (0 != (e = agg_create(&newagg, oldagg->generation + 1, parent_pool, fsinfotab)))
	{
		return e;
	}

	apr_hash_t *active_query_tab = get_active_queries(newagg->pool);
	if (! active_query_tab)
	{
		agg_destroy(newagg);
		return APR_EINVAL;
	}

	for (hi = apr_hash_first(0, oldagg->qtab); hi; hi = apr_hash_next(hi))
	{
		void* vptr;
		qdnode_t* dp;
		qdnode_t* newdp;

		apr_hash_this(hi, 0, 0, &vptr);
		dp = vptr;
		if (dp->recorded)
			continue;
		if ( (dp->qlog.status == GPMON_QLOG_STATUS_DONE || dp->qlog.status == GPMON_QLOG_STATUS_ERROR) &&
				(dp->qlog.tfin > 0 && ((dp->qlog.tfin - dp->qlog.tstart) < min_query_time )))
		{
			TR2(("agg_dup: skip short query %d-%d-%d generation %d, current generation %d, recorded %d\n",
						dp->qlog.key.tmid, dp->qlog.key.ssid, dp->qlog.key.ccnt,
						(int) dp->last_updated_generation, (int) newagg->generation, dp->recorded));
			continue;
		}
		if (dp->qlog.status == GPMON_QLOG_STATUS_INVALID || dp->qlog.status == GPMON_QLOG_STATUS_SILENT)
			continue;

		apr_int32_t age = newagg->generation - dp->last_updated_generation - 1;
		if (age > 0)
		{
			if (((age % 5 == 0) /* don't call is_query_not_active every time because it's expensive */
			       && is_query_not_active(dp->qlog.key, active_query_tab, newagg->pool)))
			{
				if (dp->qlog.dbid != gpperfmon_dbid)
				{
					TR2(("agg_dup: skip %d-%d-%d generation %d, current generation %d, recorded %d\n",
						dp->qlog.key.tmid, dp->qlog.key.ssid, dp->qlog.key.ccnt,
						(int) dp->last_updated_generation, (int) newagg->generation, dp->recorded));
				}
				continue;
			}
		}

		if (dp->qlog.dbid != gpperfmon_dbid) {
			TR2( ("agg_dup: add %d-%d-%d, generation %d, recorded %d:\n", dp->qlog.key.tmid, dp->qlog.key.ssid, dp->qlog.key.ccnt, (int) dp->last_updated_generation, dp->recorded));
		}

		/* dup this entry */
		if (!(newdp = apr_palloc(newagg->pool, sizeof(*newdp)))) {
			agg_destroy(newagg);
			return APR_ENOMEM;
		}

		*newdp = *dp;

                newdp->num_metrics_packets_interval = 0;
                newdp->host_cnt = 0;
                memset(&newdp->p_interval_metrics, 0, sizeof(newdp->p_interval_metrics));
                memset(&newdp->p_queries_now_metrics, 0, sizeof(newdp->p_queries_now_metrics));

		newdp->query_seginfo_hash = apr_hash_make(newagg->pool);
		if (!newdp->query_seginfo_hash) {
			agg_destroy(newagg);
			return APR_ENOMEM;
		}
                newdp->host_hash = apr_hash_make(newagg->pool);
                if (!newdp->host_hash) {
                        agg_destroy(newagg);
                        return APR_ENOMEM;
                }

		cnt = 0;
		// Copy the query_seginfo hash table
		for (hj = apr_hash_first(newagg->pool, dp->query_seginfo_hash); hj; hj = apr_hash_next(hj)) {
			mmon_query_seginfo_t* new_query_seginfo;
			apr_hash_this(hj, 0, 0, &vptr);

			if (!(new_query_seginfo = apr_pcalloc(newagg->pool, sizeof(mmon_query_seginfo_t)))) {
				agg_destroy(newagg);
				return APR_ENOMEM;
			}
			*new_query_seginfo = *((mmon_query_seginfo_t*)vptr);

			apr_hash_set(newdp->query_seginfo_hash, &(new_query_seginfo->key.segid), sizeof(new_query_seginfo->key.segid), new_query_seginfo);
		}

		apr_hash_set(newagg->qtab, &newdp->qlog.key, sizeof(newdp->qlog.key), newdp);
	}

	*retagg = newagg;
	return 0;
}

void agg_destroy(agg_t* agg)
{
	apr_pool_destroy(agg->pool);
}

apr_status_t agg_put(agg_t* agg, gp_smon_to_mmon_packet_t* pkt)
{
	set_tmid(pkt, tmid);
	if (pkt->header.pkttype == GPMON_PKTTYPE_METRICS)
		return agg_put_metrics(agg, &pkt->u.metrics);
	if (pkt->header.pkttype == GPMON_PKTTYPE_QLOG)
		return agg_put_qlog(agg, &pkt->u.qlog, agg->generation);
	if (pkt->header.pkttype == GPMON_PKTTYPE_QUERY_HOST_METRICS)
		return agg_put_query_metrics(agg, &pkt->u.qlog, agg->generation, pkt->ipaddr);
	if (pkt->header.pkttype == GPMON_PKTTYPE_FSINFO)
		return agg_put_fsinfo(agg, &pkt->u.fsinfo);
	if (pkt->header.pkttype == GPMON_PKTTYPE_QUERYSEG)
		return agg_put_queryseg(agg, &pkt->u.queryseg, agg->generation);

	gpmon_warning(FLINE, "unknown packet type %d", pkt->header.pkttype);
	return 0;
}


typedef struct bloom_t bloom_t;
struct bloom_t {
	unsigned char map[1024];
};
static void bloom_init(bloom_t* bloom);
static void bloom_set(bloom_t* bloom, const char* name);
static int  bloom_isset(bloom_t* bloom, const char* name);
static void delete_old_files(bloom_t* bloom);
static apr_uint32_t write_fsinfo(agg_t* agg, const char* nowstr);
static apr_uint32_t write_system(agg_t* agg, const char* nowstr);
static apr_uint32_t write_segmentinfo(agg_t* agg, char* nowstr);
static apr_uint32_t write_dbmetrics(dbmetrics_t* dbmetrics, char* nowstr);
static apr_uint32_t write_qlog(FILE* fp, qdnode_t *qdnode, const char* nowstr, apr_uint32_t done);
static apr_uint32_t write_qlog_full(FILE* fp, qdnode_t *qdnode, const char* nowstr, apr_pool_t* pool);

apr_status_t agg_dump(agg_t* agg)
{
	apr_hash_index_t *hi;
	bloom_t bloom;
	char nowstr[GPMON_DATE_BUF_SIZE];
	FILE* fp_queries_now = 0;
	FILE* fp_queries_tail = 0;
        apr_hash_t *spill_file_tab = NULL;
	dbmetrics_t dbmetrics = {0};

	apr_uint32_t temp_bytes_written = 0;

	gpmon_datetime_rounded(time(NULL), nowstr);

	bloom_init(&bloom);

	/* we never delete system_tail/ system_now/
		queries_tail/ queries_now/ files */
	bloom_set(&bloom, GPMON_DIR "system_now.dat");
	bloom_set(&bloom, GPMON_DIR "system_tail.dat");
	bloom_set(&bloom, GPMON_DIR "system_stage.dat");
	bloom_set(&bloom, GPMON_DIR "_system_tail.dat");
	bloom_set(&bloom, GPMON_DIR "queries_now.dat");
	bloom_set(&bloom, GPMON_DIR "queries_tail.dat");
	bloom_set(&bloom, GPMON_DIR "queries_stage.dat");
	bloom_set(&bloom, GPMON_DIR "_queries_tail.dat");
	bloom_set(&bloom, GPMON_DIR "diskspace_now.dat");
	bloom_set(&bloom, GPMON_DIR "diskspace_tail.dat");
	bloom_set(&bloom, GPMON_DIR "diskspace_stage.dat");
	bloom_set(&bloom, GPMON_DIR "_diskspace_tail.dat");
        // get spill file size
        spill_file_tab = gpdb_get_spill_file_size(agg->pool);

	/* dump metrics */
	temp_bytes_written = write_system(agg, nowstr);
	incremement_tail_bytes(temp_bytes_written);

	/* write segment metrics */
	temp_bytes_written = write_segmentinfo(agg, nowstr);
	incremement_tail_bytes(temp_bytes_written);

	/* write fsinfo metrics */
	temp_bytes_written = write_fsinfo(agg, nowstr);
	incremement_tail_bytes(temp_bytes_written);

	if (! (fp_queries_tail = fopen(GPMON_DIR "queries_tail.dat", "a")))
		return APR_FROM_OS_ERROR(errno);

	/* loop through queries */
	for (hi = apr_hash_first(0, agg->qtab); hi; hi = apr_hash_next(hi))
	{
		void* vptr;
		qdnode_t* qdnode;
		apr_hash_this(hi, 0, 0, &vptr);
		qdnode = vptr;

		if (qdnode->qlog.status == GPMON_QLOG_STATUS_DONE || qdnode->qlog.status == GPMON_QLOG_STATUS_ERROR)
		{
			if (!qdnode->recorded && ((qdnode->qlog.tfin - qdnode->qlog.tstart) >= min_query_time))
			{
                                update_query_history_metrics(qdnode);
				temp_bytes_written += write_qlog_full(fp_queries_tail, qdnode, nowstr, agg->pool);
				incremement_tail_bytes(temp_bytes_written);

				qdnode->recorded = 1;
			}
		}
		else
		{
			switch (qdnode->qlog.status)
			{
			case GPMON_QLOG_STATUS_START:
			case GPMON_QLOG_STATUS_CANCELING:
				dbmetrics.queries_running++;
				break;
			case GPMON_QLOG_STATUS_SUBMIT:
				dbmetrics.queries_queued++;
				break;
			default:
				/* Not interested */
				break;
			}
		}
	}
	dbmetrics.queries_total = dbmetrics.queries_running + dbmetrics.queries_queued;

	fclose(fp_queries_tail);
	fp_queries_tail = 0;

	/* dump dbmetrics */
	temp_bytes_written += write_dbmetrics(&dbmetrics, nowstr);
	incremement_tail_bytes(temp_bytes_written);

	if (! (fp_queries_now = fopen(GPMON_DIR "_queries_now.dat", "w")))
		return APR_FROM_OS_ERROR(errno);

	for (hi = apr_hash_first(0, agg->qtab); hi; hi = apr_hash_next(hi))
	{
		void* vptr;
		qdnode_t* qdnode;

		apr_hash_this(hi, 0, 0, &vptr);
		qdnode = vptr;

		/* don't touch this file */
		{
			const int fname_size = sizeof(GPMON_DIR) + 100;
			char fname[fname_size];
			get_query_text_file_name(qdnode->qlog.key, fname);
			bloom_set(&bloom, fname);
		}

                long *spill_file_size = NULL;
                if (spill_file_tab != NULL)
                {
                        char *key = apr_psprintf(agg->pool, "%d-%d", qdnode->qlog.key.ssid, qdnode->qlog.key.ccnt);
                        spill_file_size = apr_hash_get(spill_file_tab, key, APR_HASH_KEY_STRING);
                }

		/* write to _query_now.dat */
		if (qdnode->qlog.status != GPMON_QLOG_STATUS_DONE && qdnode->qlog.status != GPMON_QLOG_STATUS_ERROR)
		{
                        update_query_now_metrics(qdnode, spill_file_size);
			write_qlog(fp_queries_now, qdnode, nowstr, 0);
		}
		else if (qdnode->qlog.tfin - qdnode->qlog.tstart >= min_query_time)
		{
                        update_query_now_metrics(qdnode, spill_file_size);
			write_qlog(fp_queries_now, qdnode, nowstr, 1);
		}

	}

	if (fp_queries_now) fclose(fp_queries_now);
	if (fp_queries_tail) fclose(fp_queries_tail);
	rename(GPMON_DIR "_system_now.dat", GPMON_DIR "system_now.dat");
	rename(GPMON_DIR "_segment_now.dat", GPMON_DIR "segment_now.dat");
	rename(GPMON_DIR "_queries_now.dat", GPMON_DIR "queries_now.dat");
	rename(GPMON_DIR "_database_now.dat", GPMON_DIR "database_now.dat");
	rename(GPMON_DIR "_diskspace_now.dat", GPMON_DIR "diskspace_now.dat");

	/* clean up ... delete all old files by checking our bloom filter */
	delete_old_files(&bloom);

	return 0;
}

extern int gpmmon_quantum(void);

static void delete_old_files(bloom_t* bloom)
{
	char findDir[256] = {0};
	char findCmd[512] = {0};
	FILE* fp = NULL;
	time_t cutoff = time(0) - gpmmon_quantum() * 3;
        cutoff = cutoff < 10 ? 10 : cutoff;

	/* Need to remove trailing / in dir so find results are consistent
     * between platforms
     */
	strncpy(findDir, GPMON_DIR, 255);
	if (findDir[strlen(findDir) -1] == '/')
		findDir[strlen(findDir) - 1] = '\0';

	snprintf(findCmd, 512, "find %s -name \"q*-*.txt\" 2> /dev/null", findDir);
	fp = popen(findCmd, "r");

	if (fp)
	{
		for (;;)
		{
			char line[1024];
			char* p;
			struct stat stbuf;
			apr_int32_t status;

			line[sizeof(line) - 1] = 0;
			if (! (p = fgets(line, sizeof(line), fp)))
				break;
			if (line[sizeof(line) - 1])
				continue; 	/* fname too long */

			p = gpmon_trim(p);
			TR2(("Checking file %s\n", p));

			if (0 == stat(p, &stbuf))
			{
#if defined(linux)
				int expired = stbuf.st_mtime < cutoff;
#else
				int expired = stbuf.st_mtimespec.tv_sec < cutoff;
#endif
				TR2(("File %s expired: %d\n", p, expired));
				if (expired)
				{
					gpmon_qlogkey_t qkey = {0};
					if (bloom_isset(bloom, p))
					{
						TR2(("File %s has bloom set.  Checking status\n", p));
						/* Verify no bloom collision */
						sscanf(p, GPMON_DIR "q%d-%d-%d.txt", &qkey.tmid, &qkey.ssid, &qkey.ccnt);
						TR2(("tmid: %d, ssid: %d, ccnt: %d\n", qkey.tmid, qkey.ssid, qkey.ccnt));
						status = get_query_status(qkey);
						TR2(("File %s has status of %d\n", p, status));
						if (status == GPMON_QLOG_STATUS_DONE ||
						   status == GPMON_QLOG_STATUS_ERROR)
						{
							TR2(("Deleting file %s\n", p));
							unlink(p);
						}
					}
					else
					{
						TR2(("Deleting file %s\n", p));
						unlink(p);
					}
				}
			}
		}
		pclose(fp);
	}
	else
	{
		gpmon_warning(FLINE, "Failed to get a list of query text files.\n");
	}
}

static apr_uint32_t write_segmentinfo(agg_t* agg, char* nowstr)
{
	FILE* fp = fopen(GPMON_DIR "segment_tail.dat", "a");
	FILE* fp2 = fopen(GPMON_DIR "_segment_now.dat", "w");
	apr_hash_index_t* hi;
	const int line_size = 256;
	char line[line_size];
	apr_uint32_t bytes_written = 0;

	if (!fp || !fp2)
	{
		if (fp) fclose(fp);
		if (fp2) fclose(fp2);
		return 0;
	}

	for (hi = apr_hash_first(0, agg->stab); hi; hi = apr_hash_next(hi))
	{
		gpmon_seginfo_t* sp;
		int bytes_this_record;
		void* valptr = 0;
		apr_hash_this(hi, 0, 0, (void**) &valptr);
		sp = (gpmon_seginfo_t*) valptr;

		snprintf(line, line_size, "%s|%d|%s|%" FMTU64 "|%" FMTU64, nowstr, sp->dbid, sp->hostname, sp->dynamic_memory_used, sp->dynamic_memory_available);

		bytes_this_record = strlen(line) + 1;
		if (bytes_this_record == line_size)
		{
			gpmon_warning(FLINE, "segmentinfo line to too long ... ignored: %s", line);
			continue;
		}
		fprintf(fp, "%s\n", line);
		fprintf(fp2, "%s\n", line);
		bytes_written += bytes_this_record;
    }

	fclose(fp);
	fclose(fp2);
	return bytes_written;
}

static apr_uint32_t write_fsinfo(agg_t* agg, const char* nowstr)
{
	FILE* fp = fopen(GPMON_DIR "diskspace_tail.dat", "a");
	FILE* fp2 = fopen(GPMON_DIR "_diskspace_now.dat", "w");
	apr_hash_index_t* hi;
	const int line_size = 512;
	char line[line_size];
	apr_uint32_t bytes_written = 0;
	static time_t last_time_fsinfo_written = 0;

	if (!fp || !fp2)
	{
		if (fp) fclose(fp);
		if (fp2) fclose(fp2);
		return 0;
	}

	for (hi = apr_hash_first(0, agg->fsinfotab); hi; hi = apr_hash_next(hi))
	{
		mmon_fsinfo_t* fsp;
		void* valptr = 0;
		int bytes_this_line;

		apr_hash_this(hi, 0, 0, (void**) &valptr);
		fsp = (mmon_fsinfo_t*) valptr;

		// We only want to write the fsinfo for packets that have been updated since the last time we wrote
		// the fsinfo, so skip the fsinfo if its timestamp is less than the last time written timestamp
		if (fsp->last_update_timestamp < last_time_fsinfo_written) {
			continue;
		}

		snprintf(line, line_size, "%s|%s|%s|%" FMT64 "|%" FMT64 "|%" FMT64,
				nowstr,
				fsp->key.hostname,
				fsp->key.fsname,
				fsp->bytes_total,
				fsp->bytes_used,
				fsp->bytes_available);

		TR2(("write_fsinfo(): writing %s\n", line));
		bytes_this_line = strlen(line) + 1;
		if (bytes_this_line == line_size){
			gpmon_warning(FLINE, "fsinfo metrics line too long ... ignored: %s", line);
			continue;
		}

		fprintf(fp, "%s\n", line);
		fprintf(fp2, "%s\n", line);

		bytes_written += bytes_this_line;
	}

	fclose(fp);
	fclose(fp2);

	last_time_fsinfo_written = time(NULL); //set the static time written variable

	return bytes_written;
}

static apr_uint32_t write_dbmetrics(dbmetrics_t* dbmetrics, char* nowstr)
{
	FILE* fp = fopen(GPMON_DIR "database_tail.dat", "a");
	FILE* fp2 = fopen(GPMON_DIR "_database_now.dat", "w");
	int e;
	const int line_size = 256;
	char line[line_size];
	int bytes_written;

	if (!fp || !fp2)
	{
		e = APR_FROM_OS_ERROR(errno);
		if (fp) fclose(fp);
		if (fp2) fclose(fp2);
		return e;
	}

	snprintf(line, line_size, "%s|%d|%d|%d", nowstr,
             dbmetrics->queries_total,
             dbmetrics->queries_running,
             dbmetrics->queries_queued);

	if (strlen(line) + 1 == line_size){
		gpmon_warning(FLINE, "dbmetrics line too long ... ignored: %s", line);
		bytes_written = 0;
	} else {
		fprintf(fp, "%s\n", line);
		fprintf(fp2, "%s\n", line);
		bytes_written = strlen(line) + 1;
	}

    fclose(fp);
    fclose(fp2);

    return bytes_written;
}

static apr_uint32_t write_system(agg_t* agg, const char* nowstr)
{
	FILE* fp = fopen(GPMON_DIR "system_tail.dat", "a");
	FILE* fp2 = fopen(GPMON_DIR "_system_now.dat", "w");
	apr_hash_index_t* hi;
	const int line_size = 1000;
	char line[line_size];
	apr_uint32_t bytes_written = 0;

	if (!fp || !fp2)
	{
		if (fp) fclose(fp);
		if (fp2) fclose(fp2);
		return 0;
 	}

	for (hi = apr_hash_first(0, agg->htab); hi; hi = apr_hash_next(hi))
	{
		gpmon_metrics_t* mp;
		void* valptr = 0;
		int quantum = gpmmon_quantum();
		int bytes_this_line;
		apr_hash_this(hi, 0, 0, (void**) &valptr);
		mp = (gpmon_metrics_t*) valptr;

		snprintf(line, line_size,
		"%s|%s|%" FMT64 "|%" FMT64 "|%" FMT64 "|%" FMT64 "|%" FMT64 "|%" FMT64 "|%" FMT64 "|%" FMT64 "|%.2f|%.2f|%.2f|%.4f|%.4f|%.4f|%d|%" FMT64 "|%" FMT64 "|%" FMT64 "|%" FMT64 "|%" FMT64 "|%" FMT64 "|%" FMT64 "|%" FMT64,
		nowstr,
		mp->hname,
		mp->mem.total,
		mp->mem.used,
		mp->mem.actual_used,
		mp->mem.actual_free,
		mp->swap.total,
		mp->swap.used,
		(apr_int64_t)ceil((double)mp->swap.page_in / (double)quantum),
		(apr_int64_t)ceil((double)mp->swap.page_out / (double)quantum),
		mp->cpu.user_pct,
		mp->cpu.sys_pct,
		mp->cpu.idle_pct,
		mp->load_avg.value[0],
		mp->load_avg.value[1],
		mp->load_avg.value[2],
		quantum,
		mp->disk.ro_rate,
		mp->disk.wo_rate,
		mp->disk.rb_rate,
		mp->disk.wb_rate,
		mp->net.rp_rate,
		mp->net.wp_rate,
		mp->net.rb_rate,
		mp->net.wb_rate);

		bytes_this_line = strlen(line) + 1;
		if (bytes_this_line == line_size){
			gpmon_warning(FLINE, "system metrics line too long ... ignored: %s", line);
			continue;
		}

		fprintf(fp, "%s\n", line);
		fprintf(fp2, "%s\n", line);

		bytes_written += bytes_this_line;
	}

	fclose(fp);
	fclose(fp2);
	return bytes_written;
}

static double get_cpu_skew(qdnode_t* qdnode)
{
        apr_hash_index_t *hi;
	apr_int64_t cpu_avg = 0;
	apr_int64_t total_cpu = 0;
        apr_int64_t max_seg_cpu_sum = 0;
        double cpu_skew = 0;
	int segcnt = 0;
        void* valptr;

	if (!qdnode)
		return 0.0f;

	for (hi = apr_hash_first(NULL, qdnode->query_seginfo_hash); hi; hi = apr_hash_next(hi))
	{
		mmon_query_seginfo_t	*rec;
		apr_hash_this(hi, 0, 0, &valptr);
		rec = (mmon_query_seginfo_t*) valptr;
                if (rec->key.segid == -1)
                        continue;

                TR2(("segment cpu elapsed %lu, queryID:%d-%d-%d, segmentID:%d \n",
                        rec->sum_cpu_elapsed, rec->key.qkey.tmid, rec->key.qkey.ssid, rec->key.qkey.ccnt, rec->key.segid));

                if (rec->sum_cpu_elapsed > max_seg_cpu_sum){
                        max_seg_cpu_sum = rec->sum_cpu_elapsed;
                };

                total_cpu += rec->sum_cpu_elapsed;
                segcnt++;
	}

	if (!segcnt) {
		TR2(("No segments for CPU skew calculation\n"));
		return 0.0f;
	}

	cpu_avg = total_cpu / segcnt;
        cpu_skew = 1 - (cpu_avg / max_seg_cpu_sum);
        TR2(("(SKEW) queryID:%d-%d-%d, Avg cpu usage: %" FMT64 ", Max segment cpu sum : %" FMT64 ", cpu skew : %lf \n",
                qdnode->qlog.key.tmid, qdnode->qlog.key.ssid, qdnode->qlog.key.ccnt, cpu_avg, max_seg_cpu_sum, cpu_skew));
        return cpu_skew;
}


/*
 *  The update_query_now_metrics function is used to update qdnode.p_queries_now_metrics.
 *  p_queries_now_metrics is calculated from p_interval_metrics and then written into the queries_now table.
 *
 *  cpu_skew: Since cpu_elapsed is an accumulated value, the cpu_skew is calculated directly using this accumulated value each time.
 *
 *  spill_file_size: The value is obtained in real - time by querying the gp_workfile_usage_per_query table.
 *  Meanwhile, the maximum value of this value is recorded in p_queries_history_metrics for subsequent writing into the queries_tail table.
 *
 *  cpu_pct: Within a time window, multiple segment hosts may send multiple packets to gpmmon. At this time, the average value within the time window needs to be calculated.
 *  Therefore, the accumulated cpu_pct value should be divided by (the total number of received packets / the number of hosts that send packets).
 *  For example, suppose three segment hosts send a total of 9 packets during a certain period. After gpmmon accumulates these 9 packets, the actual cpu_pct should be sum_cpu_pct/(9/3).
 *  Here, 9/3 means that three groups of packets are received within this time window (the sum of the packets sent by the three segment hosts is regarded as one group).
 *
 *  mem.resident: Using resident can more accurately reflect the actual physical memory value used for executing the query.
 *  Its calculation method is the same as that of cpu_pct, which will not be repeated here.
 *  At the same time, the maximum value of mem.resident is recorded for subsequent writing into the queries_tail table.
 */
static void update_query_now_metrics(qdnode_t* qdnode, long *spill_file_size)
{
        qdnode->p_queries_now_metrics.cpu_skew = get_cpu_skew(qdnode);

        if (spill_file_size != NULL)
        {
                qdnode->p_queries_now_metrics.spill_files_size = *spill_file_size;
        }

        if (qdnode->p_queries_now_metrics.spill_files_size > 0 && qdnode->p_queries_now_metrics.spill_files_size > qdnode->p_queries_history_metrics.spill_files_size)
        {
                qdnode->p_queries_history_metrics.spill_files_size = qdnode->p_queries_now_metrics.spill_files_size;
                TR2(("(SPILL FILE) queryID:%d-%d-%d, spill file size peak: %lu \n", qdnode->qlog.key.tmid,
                        qdnode->qlog.key.ssid, qdnode->qlog.key.ccnt, qdnode->p_queries_history_metrics.spill_files_size));
        }

        if (qdnode->num_metrics_packets_interval && qdnode->host_cnt)
        {
                qdnode->p_queries_now_metrics.cpu_pct = qdnode->p_interval_metrics.cpu_pct / (qdnode->num_metrics_packets_interval / qdnode->host_cnt);
                qdnode->p_queries_history_metrics.cpu_pct += qdnode->p_queries_now_metrics.cpu_pct;
                qdnode->num_cpu_pct_interval_total++;

                qdnode->p_queries_now_metrics.mem.resident = qdnode->p_interval_metrics.mem.resident / (qdnode->num_metrics_packets_interval / qdnode->host_cnt);
                if (qdnode->p_queries_now_metrics.mem.resident > qdnode->p_queries_history_metrics.mem.resident){
                        qdnode->p_queries_history_metrics.mem.resident = qdnode->p_queries_now_metrics.mem.resident;
                }
        }
        else
        {
                qdnode->p_queries_now_metrics.cpu_pct = 0;
                qdnode->p_queries_now_metrics.mem.resident = 0;
        }
}


/*
 *  The update_query_history_metrics function is used to update qdnode.p_queries_history_metrics.
 *  p_queries_history_metrics is calculated from p_query_now_metrics and then written into the queries_now table.
 *
 *  cpu_skew: As cpu_elapsed is an accumulated value, the cpu_skew is directly calculated using this accumulated value each time.
 *
 *  spill_file_size: The maximum value of the spill file size is recorded in the queries_tail table.
 *
 *  cpu_pct: The average value of cpu_pct throughout the entire lifecycle of this query is obtained by
 *  dividing the cumulative value of cpu_pct in each previous time window by the total number of time windows.
 *
 *  mem.resident:  The maximum value of mem.resident is recorded in the queries_tail table.
 */
static void update_query_history_metrics(qdnode_t* qdnode)
{
        qdnode->p_queries_history_metrics.cpu_skew = get_cpu_skew(qdnode);
        if (qdnode->num_cpu_pct_interval_total)
        {
                qdnode->p_queries_history_metrics.cpu_pct = qdnode->p_queries_history_metrics.cpu_pct / qdnode->num_cpu_pct_interval_total;
        }
        else
        {
                qdnode->p_queries_history_metrics.cpu_pct = 0.0f;
        }
}

static void fmt_qlog(char* line, const int line_size, qdnode_t* qdnode, const char* nowstr, apr_uint32_t done)
{
	char timsubmitted[GPMON_DATE_BUF_SIZE];
	char timstarted[GPMON_DATE_BUF_SIZE];
	char timfinished[GPMON_DATE_BUF_SIZE];
        double row_skew = 0.0f;
        int query_hash = 0;
        apr_int64_t rowsout = 0;
	if (qdnode->qlog.tsubmit)
	{
		gpmon_datetime((time_t)qdnode->qlog.tsubmit, timsubmitted);
	}
	else
	{
		snprintf(timsubmitted, GPMON_DATE_BUF_SIZE, "null");
	}

	if (qdnode->qlog.tstart)
	{
		gpmon_datetime((time_t)qdnode->qlog.tstart, timstarted);
	}
	else
	{
		snprintf(timstarted, GPMON_DATE_BUF_SIZE, "null");
	}

	if (done && qdnode->qlog.tfin)
	{
		gpmon_datetime((time_t)qdnode->qlog.tfin, timfinished);
	}
	else
	{
		snprintf(timfinished, GPMON_DATE_BUF_SIZE,  "null");
	}

        TR2(("fmt qlog to queries_now , queryID:%d-%d-%d, cpu pct:%f, mem_resident:%lu, spill_files_size:%lu, cpu_skew:%lf, segment host cnt:%d, pkt nums:%d\n",
                qdnode->qlog.key.tmid, qdnode->qlog.key.ssid, qdnode->qlog.key.ccnt,
                qdnode->p_queries_now_metrics.cpu_pct, qdnode->p_queries_now_metrics.mem.resident,
                qdnode->p_queries_now_metrics.spill_files_size, qdnode->p_queries_now_metrics.cpu_skew,
                qdnode->host_cnt, qdnode->num_metrics_packets_interval));

	snprintf(line, line_size, "%s|%d|%d|%d|%d|%s|%u|%d|%s|%s|%s|%s|%" FMT64 "|%" FMT64 "|%.4f|%.2f|%.2f|%d||||||%" FMTU64 "|%" FMTU64 "|%d|%d",
		nowstr,
		qdnode->qlog.key.tmid,
		qdnode->qlog.key.ssid,
		qdnode->qlog.key.ccnt,
		qdnode->qlog.pid,
		qdnode->qlog.user,
		qdnode->qlog.dbid,
		qdnode->qlog.cost,
		timsubmitted,
		timstarted,
		timfinished,
		gpmon_qlog_status_string(qdnode->qlog.status),
		rowsout,
		qdnode->qlog.cpu_elapsed,
                qdnode->p_queries_now_metrics.cpu_pct,
                qdnode->p_queries_now_metrics.cpu_skew,
		row_skew,
		query_hash,
                qdnode->p_queries_now_metrics.mem.resident,
                qdnode->p_queries_now_metrics.spill_files_size,
                0,
                0
                );
}


static apr_uint32_t write_qlog(FILE* fp, qdnode_t *qdnode, const char* nowstr, apr_uint32_t done)
{
	const int line_size = 1024;
	char line[line_size];
	int bytes_written;

	fmt_qlog(line, line_size, qdnode, nowstr, done);
	bytes_written = strlen(line) + 1;

	if (bytes_written == line_size)
	{
		gpmon_warning(FLINE, "qlog line too long ... ignored: %s", line);
		return 0;
	}
	else
	{
		fprintf(fp, "%s\n", line);
		return bytes_written;
	}
}

static int get_query_file_next_kvp(FILE* queryfd, char* qfname, char** str, apr_pool_t* pool, apr_uint32_t* bytes_written)
{
    const int line_size = 1024;
    char line[line_size];
    line[0] = 0;
    char *p = NULL;
    int field_len = 0;
    int retCode = 0;

    p = fgets(line, line_size, queryfd);
    line[line_size-1] = 0; // in case libc is buggy

    if (!p) {
	    gpmon_warning(FLINE, "Error parsing file: %s", qfname);
        return APR_NOTFOUND;
    }

    retCode = sscanf(p, "%d", &field_len);

    if (1 != retCode){
	    gpmon_warning(FLINE, "bad format on file: %s", qfname);
        return APR_NOTFOUND;
    }

    if (field_len < 0) {
	    gpmon_warning(FLINE, "bad field length on file: %s", qfname);
        return APR_NOTFOUND;
	}

    if (!field_len) {
        // empty field, read through the newline
        p = fgets(line, line_size, queryfd);
        if (p)
            return APR_SUCCESS;
        else
            return APR_NOTFOUND;
    }

    *str = apr_palloc(pool,(field_len + 1) * sizeof(char));
    memset(*str, 0, field_len+1);
    (*str)[field_len] = '\0';
    int n = fread(*str, 1, field_len, queryfd);
    if (n!= field_len)
    {
            gpmon_warning(FLINE, "missing expected bytes in file: %s", qfname);
            return APR_NOTFOUND;
    }

    n = fread(line, 1, 1, queryfd);
    if (n != 1)
    {
	    gpmon_warning(FLINE, "missing expected newline in file: %s", qfname);
        return APR_NOTFOUND;
    }

    *bytes_written += field_len;

    return APR_SUCCESS;
}

static  apr_uint32_t  get_query_info(FILE* qfptr, char qfname[], char* array[], apr_pool_t* pool)
{
        // 0 add query text
        // 1 add query plan
        // 2 add application name
        // 3 add rsqname
        // 4 add priority
        int total_iterations = 5;
        int all_good = 1;
        int iter;
        apr_uint32_t bytes_written = 0;
        int retCode = APR_SUCCESS;
        for (iter = 0; iter < total_iterations; ++iter)
        {
                if (!all_good){
                        // we have no data for query plan
                        // if we failed once already don't bother trying to parse query file
                        continue;
                }

                retCode = get_query_file_next_kvp(qfptr, qfname, &array[iter], pool, &bytes_written);
                if (retCode != APR_SUCCESS)
                        all_good = 0;
        }

        fclose(qfptr);
        return bytes_written;
}

static char* replaceQuotes(char *str, apr_pool_t* pool, int* size) {
    int len = strlen(str);
    int newLen = len;
    int quoteCount = 0;

    // count the number of quotes
    for (int i = 0; i < len; i++) {
        if (str[i] == '"') {
            quoteCount++;
        }
    }

    *size += quoteCount;

    newLen += quoteCount;
    char* newStr = apr_palloc(pool,(newLen + 1) * sizeof(char));
    int j = 0;
    for (int i = 0; i < len; i++) {
        if (str[i] == '"') {
            newStr[j++] = '"';
            newStr[j++] = '"';
        } else {
            newStr[j++] = str[i];
        }
    }
    newStr[j] = '\0';
    return newStr;
}

static void format_time(time_t tt, char *buf)
{
	if (tt)
		 gpmon_datetime(tt, buf);
	else
	 snprintf(buf, GPMON_DATE_BUF_SIZE, "-infinity");
}

static apr_uint32_t write_qlog_full(FILE* fp, qdnode_t *qdnode, const char* nowstr, apr_pool_t* pool)
{
        char timsubmitted[GPMON_DATE_BUF_SIZE];
        char timstarted[GPMON_DATE_BUF_SIZE];
        char timfinished[GPMON_DATE_BUF_SIZE];
        double row_skew = 0.0f;
        int query_hash = 0;
        apr_int64_t rowsout = 0;
		format_time(qdnode->qlog.tsubmit, timsubmitted);
		format_time(qdnode->qlog.tstart, timstarted);
		format_time(qdnode->qlog.tfin, timfinished);

        // get query text and plan
        char* array[5] = {"", "", "", "", ""};
        const int qfname_size = 256;
        char qfname[qfname_size];
        int size = 0;
        FILE* qfptr = 0;
		get_query_text_file_name(qdnode->qlog.key, qfname);
		qfptr = fopen(qfname, "r");
        if (qfptr)
        {
                // array[0] query text
                // array[1] query plan
                // array[2] application name
                // array[3] add rsqname
                // array[4] add priority
                size = get_query_info(qfptr, qfname, array, pool);
                array[0] = replaceQuotes(array[0], pool, &size);
                array[1] = replaceQuotes(array[1], pool, &size);
        }
		else
		{
			gpmon_warning(FLINE, "missing expected qyuery file: %s", qfname);
		}

        TR2(("fmt qlog to queries_tail, queryID:%d-%d-%d, cpu pct:%f, mem_resident_peak:%lu, spill_files_size_peak:%lu, cpu_skew:%lf, cpu_pct interval nums:%d\n",
                qdnode->qlog.key.tmid, qdnode->qlog.key.ssid, qdnode->qlog.key.ccnt,
                qdnode->p_queries_history_metrics.cpu_pct, qdnode->p_queries_history_metrics.mem.resident,
                qdnode->p_queries_history_metrics.spill_files_size, qdnode->p_queries_history_metrics.cpu_skew, qdnode->num_cpu_pct_interval_total));

        int line_size = (1024+size)*sizeof(char);
        char* line = apr_palloc(pool,line_size);
        memset(line,0,line_size);
        snprintf(line, line_size, "%s|%d|%d|%d|%d|%s|%u|%d|%s|%s|%s|%s|%" FMT64 "|%" FMT64 "|%.4f|%.2f|%.2f|%d|\"%s\"|\"%s\"|\"%s\"|\"%s\"|\"%s\"|%" FMTU64 "|%" FMTU64 "|%d|%d",
                nowstr,
                qdnode->qlog.key.tmid,
                qdnode->qlog.key.ssid,
                qdnode->qlog.key.ccnt,
                qdnode->qlog.pid,
                qdnode->qlog.user,
                qdnode->qlog.dbid,
                qdnode->qlog.cost,
                timsubmitted,
                timstarted,
                timfinished,
                gpmon_qlog_status_string(qdnode->qlog.status),
                rowsout,
                qdnode->qlog.cpu_elapsed,
                qdnode->p_queries_history_metrics.cpu_pct,
                qdnode->p_queries_history_metrics.cpu_skew,
                row_skew,
                query_hash,
                array[0],
                array[1],
                array[2],
                array[3],
                array[4],
                qdnode->p_queries_history_metrics.mem.resident,
                qdnode->p_queries_history_metrics.spill_files_size,
                0,
                0
        );

        fprintf(fp, "%s\n", line);
        apr_uint32_t bytes_written = strlen(line) + 1;
        return bytes_written;
}

static void bloom_init(bloom_t* bloom)
{
    memset(bloom->map, 0, sizeof(bloom->map));
}

static void bloom_set(bloom_t* bloom, const char* name)
{
    apr_ssize_t namelen = strlen(name);
    const unsigned int hashval =
	apr_hashfunc_default(name, &namelen) % (8 * sizeof(bloom->map));
    const int idx = hashval / 8;
    const int off = hashval % 8;
    /* printf("bloom set %s h%d\n", name, hashval); */
    bloom->map[idx] |= (1 << off);
}

static int bloom_isset(bloom_t* bloom, const char* name)
{
    apr_ssize_t namelen = strlen(name);
    const unsigned int hashval =
	apr_hashfunc_default(name, &namelen) % (8 * sizeof(bloom->map));
    const int idx = hashval / 8;
    const int off = hashval % 8;
    /*
      printf("bloom check %s h%d = %d\n", name, hashval,
      0 != (bloom->map[idx] & (1 << off)));
    */
    return 0 != (bloom->map[idx] & (1 << off));
}

static void
set_tmid(gp_smon_to_mmon_packet_t* pkt, int32 tmid)
{

	if (pkt->header.pkttype == GPMON_PKTTYPE_QLOG ||
		pkt->header.pkttype == GPMON_PKTTYPE_QUERY_HOST_METRICS)
	{
		gpmon_qlog_t* qlog = &(pkt->u.qlog);
		qlog->key.tmid = tmid;
	}
	if (pkt->header.pkttype == GPMON_PKTTYPE_QUERYSEG)
	{
		gpmon_query_seginfo_t* met = &pkt->u.queryseg;
		met->key.qkey.tmid = tmid;
	}
}
