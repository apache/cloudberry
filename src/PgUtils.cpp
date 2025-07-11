#include "PgUtils.h"
#include "Config.h"
#include "memory/gpdbwrappers.h"

extern "C" {
#include "commands/resgroupcmds.h"
#include "cdb/cdbvars.h"
}

std::string *get_user_name() {
  const char *username =
      gpdb::get_config_option("session_authorization", false, false);
  // username is not to be freed
  return username ? new std::string(username) : nullptr;
}

std::string *get_db_name() {
  char *dbname = gpdb::get_database_name(MyDatabaseId);
  std::string *result = nullptr;
  if (dbname) {
    result = new std::string(dbname);
    gpdb::pfree(dbname);
  }
  return result;
}

std::string *get_rg_name() {
  auto groupId = gpdb::get_rg_id_by_session_id(MySessionState->sessionId);
  if (!OidIsValid(groupId))
    return nullptr;
  char *rgname = gpdb::get_rg_name_for_id(groupId);
  if (rgname == nullptr)
    return nullptr;
  return new std::string(rgname);
}

/**
 * Things get tricky with nested queries.
 * a) A nested query on master is a real query optimized and executed from
 * master. An example would be `select some_insert_function();`, where
 * some_insert_function does something like `insert into tbl values (1)`. Master
 * will create two statements. Outer select statement and inner insert statement
 * with nesting level 1.
 * For segments both statements are top-level statements with nesting level 0.
 * b) A nested query on segment is something executed as sub-statement on
 * segment. An example would be `select a from tbl where is_good_value(b);`. In
 * this case master will issue one top-level statement, but segments will change
 * contexts for UDF execution and execute  is_good_value(b) once for each tuple
 * as a nested query. Creating massive load on gpcc agent.
 *
 * Hence, here is a decision:
 * 1) ignore all queries that are nested on segments
 * 2) record (if enabled) all queries that are nested on master
 * NODE: The truth is, we can't really ignore nested master queries, because
 * segment sees those as top-level.
 */

bool is_top_level_query(QueryDesc *query_desc, int nesting_level) {
  return (query_desc->gpmon_pkt &&
          query_desc->gpmon_pkt->u.qexec.key.tmid == 0) ||
         nesting_level == 0;
}

bool nesting_is_valid(QueryDesc *query_desc, int nesting_level) {
  return (Gp_session_role == GP_ROLE_DISPATCH &&
          Config::report_nested_queries()) ||
         is_top_level_query(query_desc, nesting_level);
}

bool need_report_nested_query() {
  return Config::report_nested_queries() && Gp_session_role == GP_ROLE_DISPATCH;
}

bool filter_query(QueryDesc *query_desc) {
  return gp_command_count == 0 || query_desc->sourceText == nullptr ||
         !Config::enable_collector() || Config::filter_user(get_user_name());
}

bool need_collect(QueryDesc *query_desc, int nesting_level) {
  return !filter_query(query_desc) &&
         nesting_is_valid(query_desc, nesting_level);
}
