#include "Config.h"
#include <unordered_set>
#include <memory>
#include <string>

extern "C" {
#include "postgres.h"
#include "utils/builtins.h"
#include "utils/guc.h"
}

static char *guc_uds_path = nullptr;
static bool guc_enable_analyze = true;
static bool guc_enable_cdbstats = true;
static bool guc_enable_collector = true;
static char *guc_ignored_users = nullptr;
static std::unique_ptr<std::unordered_set<std::string>> ignored_users = nullptr;

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

  DefineCustomStringVariable(
      "yagpcc.ignored_users_list",
      "Make yagpcc ignore queries issued by given users", 0LL,
      &guc_ignored_users, "gpadmin,repl,gpperfmon,monitor", PGC_SUSET,
      GUC_NOT_IN_SAMPLE | GUC_GPDB_NEED_SYNC, 0LL, 0LL, 0LL);
}

std::string Config::uds_path() { return guc_uds_path; }
bool Config::enable_analyze() { return guc_enable_analyze; }
bool Config::enable_cdbstats() { return guc_enable_cdbstats; }
bool Config::enable_collector() { return guc_enable_collector; }

bool Config::filter_user(const std::string &username) {
  if (!ignored_users) {
    ignored_users.reset(new std::unordered_set<std::string>());
    if (guc_ignored_users == nullptr || guc_ignored_users[0] == '0') {
      return false;
    }
    /* Need a modifiable copy of string */
    char *rawstring = pstrdup(guc_ignored_users);
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
      return false;
    }
    foreach (l, elemlist) {
      ignored_users->insert((char *)lfirst(l));
    }
    pfree(rawstring);
    list_free(elemlist);
  }
  return ignored_users->find(username) != ignored_users->end();
}
