#ifndef GPMON_AGG_H
#define GPMON_AGG_H

#include "apr_pools.h"
#include "gpmonlib.h"

typedef struct agg_t agg_t;
apr_status_t agg_create(agg_t** retagg, apr_int64_t generation, apr_pool_t* parent_pool, apr_hash_t* fsinfotab);
apr_status_t agg_dup(agg_t** agg, agg_t* oldagg, apr_pool_t* pool, apr_hash_t* fsinfotab);
void agg_destroy(agg_t* agg);
apr_status_t agg_put(agg_t* agg, gp_smon_to_mmon_packet_t* pkt);
apr_status_t agg_dump(agg_t* agg);
typedef struct qdnode_t {
        apr_int64_t last_updated_generation;
        int recorded;

        // num_cpu_pct_interval_total represents how many time windows cpu_pct has gone through
        // from being written into queries_now to being written into queries_tail.
        int num_cpu_pct_interval_total;

        //The total number of packets received in a certain time window. When the metrics are written into queries_now, this value will be reset to 0.
        //The definition of a time window is the period from the last time when queries_now was written to the current time when queries_now is written.
        int num_metrics_packets_interval;

        //The p_interval_metrics records the instantaneous values of the metrics within a certain time window.
        gpmon_proc_metrics_t p_interval_metrics;
        //The p_queries_now_metrics is calculated from p_interval_metrics and then written into the queries_now table.
        gpmon_proc_metrics_t p_queries_now_metrics;
        //The p_queries_history_metrics is calculated from p_now_metrics and then written into the queries_tail table.
        gpmon_proc_metrics_t p_queries_history_metrics;
        int host_cnt;
        gpmon_qlog_t qlog;
        apr_hash_t* host_hash;
        apr_hash_t* query_seginfo_hash;
} qdnode_t;
#endif
