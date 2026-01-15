#pragma once

#include <array>
#include <string>
#include <string_view>
#include <unordered_map>

extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
}

namespace google {
namespace protobuf {
class FieldDescriptor;
class Message;
class Reflection;
class Timestamp;
} // namespace protobuf
} // namespace google

inline constexpr std::string_view schema_name = "yagpcc";
inline constexpr std::string_view log_relname = "__log";

struct LogDesc {
  std::string_view pg_att_name;
  std::string_view proto_field_name;
  Oid type_oid;
};

/*
 * Definition of the log table structure.
 *
 * System stats collected as %lu (unsigned) may
 * overflow INT8OID (signed), but this is acceptable.
 */
/* clang-format off */
inline constexpr std::array log_tbl_desc = {  
 /* 8-byte aligned types first - Query Info */  
 LogDesc{"query_id",             "query_info.query_id",   INT8OID},  
 LogDesc{"plan_id",              "query_info.plan_id",    INT8OID},  
 LogDesc{"nested_level",         "add_info.nested_level", INT8OID},  
 LogDesc{"slice_id",             "add_info.slice_id",     INT8OID},  
 /* 8-byte aligned types - System Stats */  
 LogDesc{"systemstat_vsize",                  "query_metrics.systemStat.vsize",                 INT8OID},  
 LogDesc{"systemstat_rss",                    "query_metrics.systemStat.rss",                   INT8OID},  
 LogDesc{"systemstat_vmsizekb",               "query_metrics.systemStat.VmSizeKb",              INT8OID},  
 LogDesc{"systemstat_vmpeakkb",               "query_metrics.systemStat.VmPeakKb",              INT8OID},  
 LogDesc{"systemstat_rchar",                  "query_metrics.systemStat.rchar",                 INT8OID},  
 LogDesc{"systemstat_wchar",                  "query_metrics.systemStat.wchar",                 INT8OID},  
 LogDesc{"systemstat_syscr",                  "query_metrics.systemStat.syscr",                 INT8OID},  
 LogDesc{"systemstat_syscw",                  "query_metrics.systemStat.syscw",                 INT8OID},  
 LogDesc{"systemstat_read_bytes",             "query_metrics.systemStat.read_bytes",            INT8OID},  
 LogDesc{"systemstat_write_bytes",            "query_metrics.systemStat.write_bytes",           INT8OID},  
 LogDesc{"systemstat_cancelled_write_bytes",  "query_metrics.systemStat.cancelled_write_bytes", INT8OID},  
 /* 8-byte aligned types - Metric Instrumentation */  
 LogDesc{"instrumentation_ntuples",                  "query_metrics.instrumentation.ntuples",             INT8OID},  
 LogDesc{"instrumentation_nloops",                   "query_metrics.instrumentation.nloops",              INT8OID},  
 LogDesc{"instrumentation_tuplecount",               "query_metrics.instrumentation.tuplecount",          INT8OID},  
 LogDesc{"instrumentation_shared_blks_hit",          "query_metrics.instrumentation.shared_blks_hit",     INT8OID},  
 LogDesc{"instrumentation_shared_blks_read",         "query_metrics.instrumentation.shared_blks_read",    INT8OID},  
 LogDesc{"instrumentation_shared_blks_dirtied",      "query_metrics.instrumentation.shared_blks_dirtied", INT8OID},  
 LogDesc{"instrumentation_shared_blks_written",      "query_metrics.instrumentation.shared_blks_written", INT8OID},  
 LogDesc{"instrumentation_local_blks_hit",           "query_metrics.instrumentation.local_blks_hit",      INT8OID},  
 LogDesc{"instrumentation_local_blks_read",          "query_metrics.instrumentation.local_blks_read",     INT8OID},  
 LogDesc{"instrumentation_local_blks_dirtied",       "query_metrics.instrumentation.local_blks_dirtied",  INT8OID},  
 LogDesc{"instrumentation_local_blks_written",       "query_metrics.instrumentation.local_blks_written",  INT8OID},  
 LogDesc{"instrumentation_temp_blks_read",           "query_metrics.instrumentation.temp_blks_read",      INT8OID},  
 LogDesc{"instrumentation_temp_blks_written",        "query_metrics.instrumentation.temp_blks_written",   INT8OID},  
 LogDesc{"instrumentation_inherited_calls",          "query_metrics.instrumentation.inherited_calls",     INT8OID},  
 /* 8-byte aligned types - Network Stats */  
 LogDesc{"instrumentation_sent_total_bytes",         "query_metrics.instrumentation.sent.total_bytes",     INT8OID},  
 LogDesc{"instrumentation_sent_tuple_bytes",         "query_metrics.instrumentation.sent.tuple_bytes",     INT8OID},  
 LogDesc{"instrumentation_sent_chunks",              "query_metrics.instrumentation.sent.chunks",          INT8OID},  
 LogDesc{"instrumentation_received_total_bytes",     "query_metrics.instrumentation.received.total_bytes", INT8OID},  
 LogDesc{"instrumentation_received_tuple_bytes",     "query_metrics.instrumentation.received.tuple_bytes", INT8OID},  
 LogDesc{"instrumentation_received_chunks",          "query_metrics.instrumentation.received.chunks",      INT8OID},  
 /* 8-byte aligned types - Interconnect Stats and spilled bytes */  
 LogDesc{"interconnect_total_recv_queue_size",         "query_metrics.instrumentation.interconnect.total_recv_queue_size",         INT8OID},  
 LogDesc{"interconnect_recv_queue_size_counting_time", "query_metrics.instrumentation.interconnect.recv_queue_size_counting_time", INT8OID},  
 LogDesc{"interconnect_total_capacity",                "query_metrics.instrumentation.interconnect.total_capacity",                INT8OID},  
 LogDesc{"interconnect_capacity_counting_time",        "query_metrics.instrumentation.interconnect.capacity_counting_time",        INT8OID},  
 LogDesc{"interconnect_total_buffers",                 "query_metrics.instrumentation.interconnect.total_buffers",                 INT8OID},  
 LogDesc{"interconnect_buffer_counting_time",          "query_metrics.instrumentation.interconnect.buffer_counting_time",          INT8OID},  
 LogDesc{"interconnect_active_connections_num",        "query_metrics.instrumentation.interconnect.active_connections_num",        INT8OID},  
 LogDesc{"interconnect_retransmits",                   "query_metrics.instrumentation.interconnect.retransmits",                   INT8OID},  
 LogDesc{"interconnect_startup_cached_pkt_num",        "query_metrics.instrumentation.interconnect.startup_cached_pkt_num",        INT8OID},  
 LogDesc{"interconnect_mismatch_num",                  "query_metrics.instrumentation.interconnect.mismatch_num",                  INT8OID},  
 LogDesc{"interconnect_crc_errors",                    "query_metrics.instrumentation.interconnect.crc_errors",                    INT8OID},  
 LogDesc{"interconnect_snd_pkt_num",                   "query_metrics.instrumentation.interconnect.snd_pkt_num",                   INT8OID},  
 LogDesc{"interconnect_recv_pkt_num",                  "query_metrics.instrumentation.interconnect.recv_pkt_num",                  INT8OID},  
 LogDesc{"interconnect_disordered_pkt_num",            "query_metrics.instrumentation.interconnect.disordered_pkt_num",            INT8OID},  
 LogDesc{"interconnect_duplicated_pkt_num",            "query_metrics.instrumentation.interconnect.duplicated_pkt_num",            INT8OID},  
 LogDesc{"interconnect_recv_ack_num",                  "query_metrics.instrumentation.interconnect.recv_ack_num",                  INT8OID},  
 LogDesc{"interconnect_status_query_msg_num",          "query_metrics.instrumentation.interconnect.status_query_msg_num",          INT8OID},
 LogDesc{"spill_totalbytes",                           "query_metrics.spill.totalBytes",                                           INT8OID},
 /* 8-byte aligned types - Float and Timestamp */  
 LogDesc{"systemstat_runningtimeseconds",     "query_metrics.systemStat.runningTimeSeconds",  FLOAT8OID},  
 LogDesc{"systemstat_usertimeseconds",        "query_metrics.systemStat.userTimeSeconds",     FLOAT8OID},  
 LogDesc{"systemstat_kerneltimeseconds",      "query_metrics.systemStat.kernelTimeSeconds",   FLOAT8OID},  
 LogDesc{"instrumentation_firsttuple",        "query_metrics.instrumentation.firsttuple",     FLOAT8OID},  
 LogDesc{"instrumentation_startup",           "query_metrics.instrumentation.startup",        FLOAT8OID},  
 LogDesc{"instrumentation_total",             "query_metrics.instrumentation.total",          FLOAT8OID},  
 LogDesc{"instrumentation_blk_read_time",     "query_metrics.instrumentation.blk_read_time",  FLOAT8OID},  
 LogDesc{"instrumentation_blk_write_time",    "query_metrics.instrumentation.blk_write_time", FLOAT8OID},  
 LogDesc{"instrumentation_startup_time",      "query_metrics.instrumentation.startup_time",   FLOAT8OID},  
 LogDesc{"instrumentation_inherited_time",    "query_metrics.instrumentation.inherited_time", FLOAT8OID},  
 LogDesc{"datetime",            "datetime",                       TIMESTAMPTZOID},  
 LogDesc{"submit_time",         "submit_time",                    TIMESTAMPTZOID},  
 LogDesc{"start_time",          "start_time",                     TIMESTAMPTZOID},  
 LogDesc{"end_time",            "end_time",                       TIMESTAMPTZOID},  
 /* 4-byte aligned types - Query Key */  
 LogDesc{"tmid",                "query_key.tmid",                 INT4OID},  
 LogDesc{"ssid",                "query_key.ssid",                 INT4OID},  
 LogDesc{"ccnt",                "query_key.ccnt",                 INT4OID},  
 /* 4-byte aligned types - Segment Key */  
 LogDesc{"dbid",                "segment_key.dbid",               INT4OID},  
 LogDesc{"segid",               "segment_key.segindex",           INT4OID},  
 LogDesc{"spill_filecount",     "query_metrics.spill.fileCount",  INT4OID},  
 /* Variable-length types - Query Info */  
 LogDesc{"generator",           "query_info.generator",           TEXTOID},  
 LogDesc{"query_text",          "query_info.query_text",          TEXTOID},  
 LogDesc{"plan_text",           "query_info.plan_text",           TEXTOID},  
 LogDesc{"template_query_text", "query_info.template_query_text", TEXTOID},  
 LogDesc{"template_plan_text",  "query_info.template_plan_text",  TEXTOID},  
 LogDesc{"user_name",           "query_info.userName",            TEXTOID},  
 LogDesc{"database_name",       "query_info.databaseName",        TEXTOID},  
 LogDesc{"rsgname",             "query_info.rsgname",             TEXTOID},  
 LogDesc{"analyze_text",        "query_info.analyze_text",        TEXTOID},  
 LogDesc{"error_message",       "add_info.error_message",         TEXTOID},  
 LogDesc{"query_status",        "query_status",                   TEXTOID},  
 /* Extra field */
 LogDesc{"utility",             "",                               BOOLOID},  
};
/* clang-format on */

inline constexpr size_t natts_yagp_log = log_tbl_desc.size();
inline constexpr size_t attnum_yagp_log_utility = natts_yagp_log - 1;

const std::unordered_map<std::string_view, size_t> &proto_name_to_col_idx();

TupleDesc DescribeTuple();

Datum protots_to_timestamptz(const google::protobuf::Timestamp &ts);

Datum field_to_datum(const google::protobuf::FieldDescriptor *field,
                     const google::protobuf::Reflection *reflection,
                     const google::protobuf::Message &msg);

/* Process a single proto field and store in values/nulls arrays */
void process_field(const google::protobuf::FieldDescriptor *field,
                   const google::protobuf::Reflection *reflection,
                   const google::protobuf::Message &msg,
                   const std::string &field_name, Datum *values, bool *nulls);

/*
 * Extracts values from msg into values/nulls arrays. Caller must
 * pre-init nulls[] to true (this function does net set nulls
 * to true for nested messages if parent message is missing).
 */
void extract_query_req(const google::protobuf::Message &msg,
                       const std::string &prefix, Datum *values, bool *nulls);
