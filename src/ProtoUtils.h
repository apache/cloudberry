#include "protos/yagpcc_set_service.pb.h"

struct QueryDesc;
struct ICStatistics;

google::protobuf::Timestamp current_ts();
void set_query_plan(yagpcc::SetQueryReq *req, QueryDesc *query_desc);
void set_query_text(yagpcc::SetQueryReq *req, QueryDesc *query_desc);
void clear_big_fields(yagpcc::SetQueryReq *req);
void set_query_info(yagpcc::SetQueryReq *req);
void set_qi_nesting_level(yagpcc::SetQueryReq *req, int nesting_level);
void set_qi_slice_id(yagpcc::SetQueryReq *req);
void set_qi_error_message(yagpcc::SetQueryReq *req);
void set_gp_metrics(yagpcc::GPMetrics *metrics, QueryDesc *query_desc,
                    int nested_calls, double nested_time);
void set_ic_stats(yagpcc::MetricInstrumentation *metrics,
                  const ICStatistics *ic_statistics);
yagpcc::SetQueryReq create_query_req(yagpcc::QueryStatus status);
double protots_to_double(const google::protobuf::Timestamp &ts);
void set_analyze_plan_text_json(QueryDesc *query_desc,
                                yagpcc::SetQueryReq *message);