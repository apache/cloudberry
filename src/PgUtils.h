extern "C" {
#include "postgres.h"
#include "commands/explain.h"
}

#include <string>

std::string get_user_name();
std::string get_db_name();
std::string get_rg_name();
bool is_top_level_query(QueryDesc *query_desc, int nesting_level);
