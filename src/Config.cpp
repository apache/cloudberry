#include "Config.h"
#include <limits.h>
#include <memory>
#include <string>
#include <unordered_set>

extern "C" {
#include "postgres.h"
#include "utils/builtins.h"
#include "utils/guc.h"
}

static char *guc_uds_path = nullptr;
static bool guc_enable_analyze = true;
static bool guc_enable_cdbstats = true;
static bool guc_enable_collector = true;
static bool guc_report_nested_queries = true;
static char *guc_ignored_users = nullptr;
static int guc_max_text_size = 1024; // in KB
static std::unique_ptr<std::unordered_set<std::string>> ignored_users_set =
    nullptr;
static bool ignored_users_guc_dirty = false;

static void update_ignored_users(const char *new_guc_ignored_users) {
  auto new_ignored_users_set =
      std::make_unique<std::unordered_set<std::string>>();
  if (new_guc_ignored_users != nullptr && new_guc_ignored_users[0] != '\0') {
    /* Need a modifiable copy of string */
    char *rawstring = pstrdup(new_guc_ignored_users);
    List *elemlist;
    ListCell *l;

    /* Parse string into list of identifiers */
    if (!SplitIdentifierString(rawstring, ',', &elemlist)) {
      /* syntax error in list */
      pfree(rawstring);
      list_free(elemlist);
      ereport(
          LOG,
          (errcode(ERRCODE_SYNTAX_ERROR),
           errmsg(
               "invalid list syntax in parameter yagpcc.ignored_users_list")));
      return;
    }
    foreach (l, elemlist) {
      new_ignored_users_set->insert((char *)lfirst(l));
    }
    pfree(rawstring);
    list_free(elemlist);
  }
  ignored_users_set = std::move(new_ignored_users_set);
}

static void assign_ignored_users_hook(const char *, void *) {
  ignored_users_guc_dirty = true;
}

void Config::init() {
  DefineCustomStringVariable(
      "yagpcc.uds_path", "Sets filesystem path of the agent socket", 0LL,
      &guc_uds_path, "/tmp/yagpcc_agent.sock", PGC_SUSET,
      GUC_NOT_IN_SAMPLE | GUC_GPDB_NEED_SYNC, 0LL, 0LL, 0LL);

  DefineCustomBoolVariable(
      "yagpcc.enable", "Enable metrics collector", 0LL, &guc_enable_collector,
      true, PGC_SUSET, GUC_NOT_IN_SAMPLE | GUC_GPDB_NEED_SYNC, 0LL, 0LL, 0LL);

  DefineCustomBoolVariable(
      "yagpcc.enable_analyze", "Collect analyze metrics in yagpcc", 0LL,
      &guc_enable_analyze, true, PGC_SUSET,
      GUC_NOT_IN_SAMPLE | GUC_GPDB_NEED_SYNC, 0LL, 0LL, 0LL);

  DefineCustomBoolVariable(
      "yagpcc.enable_cdbstats", "Collect CDB metrics in yagpcc", 0LL,
      &guc_enable_cdbstats, true, PGC_SUSET,
      GUC_NOT_IN_SAMPLE | GUC_GPDB_NEED_SYNC, 0LL, 0LL, 0LL);

  DefineCustomBoolVariable(
      "yagpcc.report_nested_queries", "Collect stats on nested queries", 0LL,
      &guc_report_nested_queries, true, PGC_USERSET,
      GUC_NOT_IN_SAMPLE | GUC_GPDB_NEED_SYNC, 0LL, 0LL, 0LL);

  DefineCustomStringVariable("yagpcc.ignored_users_list",
                             "Make yagpcc ignore queries issued by given users",
                             0LL, &guc_ignored_users,
                             "gpadmin,repl,gpperfmon,monitor", PGC_SUSET,
                             GUC_NOT_IN_SAMPLE | GUC_GPDB_NEED_SYNC, 0LL,
                             assign_ignored_users_hook, 0LL);

  DefineCustomIntVariable(
      "yagpcc.max_text_size",
      "Make yagpcc trim plan and query texts longer than configured size", NULL,
      &guc_max_text_size, 1024, 0, INT_MAX / 1024, PGC_SUSET,
      GUC_NOT_IN_SAMPLE | GUC_GPDB_NEED_SYNC | GUC_UNIT_KB, NULL, NULL, NULL);
}

std::string Config::uds_path() { return guc_uds_path; }
bool Config::enable_analyze() { return guc_enable_analyze; }
bool Config::enable_cdbstats() { return guc_enable_cdbstats; }
bool Config::enable_collector() { return guc_enable_collector; }
bool Config::report_nested_queries() { return guc_report_nested_queries; }
size_t Config::max_text_size() { return guc_max_text_size * 1024; }

bool Config::filter_user(const std::string *username) {
  if (!username || !ignored_users_set) {
    return true;
  }
  return ignored_users_set->find(*username) != ignored_users_set->end();
}

void Config::sync() {
  if (ignored_users_guc_dirty) {
    update_ignored_users(guc_ignored_users);
    ignored_users_guc_dirty = false;
  }
}