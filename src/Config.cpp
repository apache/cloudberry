#include "Config.h"

extern "C" {
#include "postgres.h"
#include "utils/builtins.h"
#include "utils/guc.h"
}

static char *guc_uds_path = nullptr;
static bool guc_enable_analyze = true;
static bool guc_enable_cdbstats = true;
static bool guc_enable_collector = true;

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
}

std::string Config::uds_path() { return guc_uds_path; }
bool Config::enable_analyze() { return guc_enable_analyze; }
bool Config::enable_cdbstats() { return guc_enable_cdbstats; }
bool Config::enable_collector() { return guc_enable_collector; }
