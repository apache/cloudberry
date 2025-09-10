extern "C" {
#include "postgres.h"
#include "commands/explain.h"
}

#include <string>

std::string get_user_name();
std::string get_db_name();
std::string get_rg_name();
bool is_top_level_query(QueryDesc *query_desc, int nesting_level);
bool nesting_is_valid(QueryDesc *query_desc, int nesting_level);
bool need_report_nested_query();
bool filter_query(QueryDesc *query_desc);
bool need_collect(QueryDesc *query_desc, int nesting_level);
ExplainState get_explain_state(QueryDesc *query_desc, bool costs);
ExplainState get_analyze_state(QueryDesc *query_desc, bool analyze);
