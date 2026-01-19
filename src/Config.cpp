#include "Config.h"
#include "memory/gpdbwrappers.h"
#include <limits.h>
#include <memory>
#include <string>
#include <unordered_set>

extern "C" {
#include "postgres.h"
#include "utils/guc.h"
}

static char *guc_uds_path = nullptr;
static bool guc_enable_analyze = true;
static bool guc_enable_cdbstats = true;
static bool guc_enable_collector = true;
static bool guc_report_nested_queries = true;
static char *guc_ignored_users = nullptr;
static int guc_max_text_size = 1 << 20;     // in bytes (1MB)
static int guc_max_plan_size = 1024;        // in KB
static int guc_min_analyze_time = 10000;    // in ms
static int guc_logging_mode = LOG_MODE_UDS;
static bool guc_enable_utility = false;

static const struct config_enum_entry logging_mode_options[] = {
    {"uds", LOG_MODE_UDS, false /* hidden */},
    {"tbl", LOG_MODE_TBL, false},
    {NULL, 0, false}};

static bool ignored_users_guc_dirty = false;

static void assign_ignored_users_hook(const char *, void *) {
  ignored_users_guc_dirty = true;
}

void Config::init_gucs() {
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
      "Make yagpcc trim query texts longer than configured size in bytes", NULL,
      &guc_max_text_size, 1 << 20 /* 1MB */, 0, INT_MAX, PGC_SUSET,
      GUC_NOT_IN_SAMPLE | GUC_GPDB_NEED_SYNC, NULL, NULL, NULL);

  DefineCustomIntVariable(
      "yagpcc.max_plan_size",
      "Make yagpcc trim plan longer than configured size", NULL,
      &guc_max_plan_size, 1024, 0, INT_MAX / 1024, PGC_SUSET,
      GUC_NOT_IN_SAMPLE | GUC_GPDB_NEED_SYNC | GUC_UNIT_KB, NULL, NULL, NULL);

  DefineCustomIntVariable(
      "yagpcc.min_analyze_time",
      "Sets the minimum execution time above which plans will be logged.",
      "Zero prints all plans. -1 turns this feature off.",
      &guc_min_analyze_time, 10000, -1, INT_MAX, PGC_USERSET,
      GUC_NOT_IN_SAMPLE | GUC_GPDB_NEED_SYNC | GUC_UNIT_MS, NULL, NULL, NULL);

  DefineCustomEnumVariable(
      "yagpcc.logging_mode", "Logging mode: UDS or PG Table", NULL,
      &guc_logging_mode, LOG_MODE_UDS, logging_mode_options, PGC_SUSET,
      GUC_NOT_IN_SAMPLE | GUC_GPDB_NEED_SYNC | GUC_SUPERUSER_ONLY, NULL, NULL,
      NULL);

  DefineCustomBoolVariable(
      "yagpcc.enable_utility", "Collect utility statement stats", NULL,
      &guc_enable_utility, false, PGC_USERSET,
      GUC_NOT_IN_SAMPLE | GUC_GPDB_NEED_SYNC, NULL, NULL, NULL);
}

void Config::update_ignored_users(const char *new_guc_ignored_users) {
  auto new_ignored_users_set = std::make_unique<IgnoredUsers>();
  if (new_guc_ignored_users != nullptr && new_guc_ignored_users[0] != '\0') {
    /* Need a modifiable copy of string */
    char *rawstring = ya_gpdb::pstrdup(new_guc_ignored_users);
    List *elemlist;
    ListCell *l;

    /* Parse string into list of identifiers */
    if (!ya_gpdb::split_identifier_string(rawstring, ',', &elemlist)) {
      /* syntax error in list */
      ya_gpdb::pfree(rawstring);
      ya_gpdb::list_free(elemlist);
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
    ya_gpdb::pfree(rawstring);
    ya_gpdb::list_free(elemlist);
  }
  ignored_users_ = std::move(new_ignored_users_set);
}

bool Config::filter_user(const std::string &username) const {
  if (!ignored_users_) {
    return true;
  }
  return ignored_users_->find(username) != ignored_users_->end();
}

void Config::sync() {
  if (ignored_users_guc_dirty) {
    update_ignored_users(guc_ignored_users);
    ignored_users_guc_dirty = false;
  }
  uds_path_ = guc_uds_path;
  enable_analyze_ = guc_enable_analyze;
  enable_cdbstats_ = guc_enable_cdbstats;
  enable_collector_ = guc_enable_collector;
  enable_utility_ = guc_enable_utility;
  report_nested_queries_ = guc_report_nested_queries;
  max_text_size_ = static_cast<size_t>(guc_max_text_size);
  max_plan_size_ = static_cast<size_t>(guc_max_plan_size);
  min_analyze_time_ = guc_min_analyze_time;
  logging_mode_ = guc_logging_mode;
}
