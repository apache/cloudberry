#include "postgres.h"
#include "access/table.h"
#include "access/xact.h"
#include "access/hash.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_database.h"
#include "cdb/cdbvars.h"
#include "commands/dbcommands.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pg_config_ext.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "tcop/utility.h"

#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

/**
 * gp_relaccess_stats collects runtime access stats on db objects: relations and
 * views. Stats include last read and write timestamps, last user, last known
 * relname and number of select, insert, update, delete or truncate queries.
 * Only committed actions are recorded.
 *
 * To track those actions we use:
 * - ExecutorCheckPerms hook for select, insert, update and delete statements
 * - ProcessUtility hook for truncate statements
 *
 * Intermediate data is stored in three hash tables.
 * One lives in shared memory and is cleaned only when dumped to disc:
 * - relaccesses - represents all recorded accesses since last dump to disc.
 * And two live in coordinator`s local memory and are cleaned on every commit
 * or rollback:
 * - local_access_entries - represent all record accesses in for this
 * transaction only
 * - relname_cache - maps relid to relname for relations used in this
 * transaction only
 *
 * Ultimately all recorded stats should end up in relaccess_stats table when a
 * user executes relaccess_stats_update(). But any intermediate stats will be
 * dumped to disc. This might happen for either or those reasons:
 * - shmem is exceeded
 * - server is restarted
 * - manual execution of relaccess_stats_dump()
 * In this case stats are offloaded to disc into pg_stat directory into separate
 * file per each tracked database: pg_stat/relaccess_stats_dump_<dbid>.csv Those
 * files are upserted into relaccess_stats when relaccess_stats_update() is
 * called
 */

PG_MODULE_MAGIC;

void _PG_init(void);
PG_FUNCTION_INFO_V1(relaccess_stats_update);
PG_FUNCTION_INFO_V1(relaccess_stats_dump);
PG_FUNCTION_INFO_V1(relaccess_stats_fillfactor);
PG_FUNCTION_INFO_V1(relaccess_stats_from_dump);

static void relaccess_stats_update_internal(void);
static void relaccess_dump_to_files(bool only_this_db);
static void relaccess_dump_to_files_internal(HTAB *files);
static void relaccess_upsert_from_file(void);
static void relaccess_shmem_request(void);
static void relaccess_shmem_startup(void);
static void relaccess_shmem_shutdown(int code, Datum arg);
static uint32 relaccess_hash_fn(const void *key, Size keysize);
static int relaccess_match_fn(const void *key1, const void *key2, Size keysize);
static uint32 local_relaccess_hash_fn(const void *key, Size keysize);
static int local_relaccess_match_fn(const void *key1, const void *key2,
                                    Size keysize);
static bool collect_relaccess_hook(List *rangeTable, List *rtePermInfos,
                                   bool ereport_on_violation);
static void relaccess_xact_callback(XactEvent event, void *arg);
static void collect_truncate_hook(PlannedStmt *pstmt, const char *queryString,
                                  bool readOnlyTree,
                                  ProcessUtilityContext context,
                                  ParamListInfo params,
                                  QueryEnvironment *queryEnv,
                                  DestReceiver *dest, QueryCompletion *qc);
static void relaccess_executor_end_hook(QueryDesc *query_desc);
static void relaccess_drop_hook(ObjectAccessType access, Oid classId,
                                Oid objectId, int subId, void *arg);
static void memorize_local_access_entry(Oid relid, AclMode perms);
static void update_relname_cache(Oid relid, char *relname);
static StringInfoData get_dump_filename(Oid dbid);

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorCheckPerms_hook_type prev_check_perms_hook = NULL;
static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;
static object_access_hook_type prev_object_access_hook = NULL;

typedef struct relaccessHashKey {
  Oid dbid;
  Oid relid;
} relaccessHashKey;

typedef struct relaccessEntry {
  relaccessHashKey key;
  char relname[NAMEDATALEN];
  Oid last_reader_id;
  Oid last_writer_id;
  TimestampTz last_read;
  TimestampTz last_write;
  int64 n_select;
  int64 n_insert;
  int64 n_update;
  int64 n_delete;
  int64 n_truncate;
} relaccessEntry;

typedef struct relaccessGlobalData {
  LWLock *relaccess_ht_lock;
  LWLock *relaccess_file_lock;
} relaccessGlobalData;

typedef struct localAccessKey {
  Oid relid;
  int stmt_cnt;
} localAccessKey;

typedef struct localAccessEntry {
  localAccessKey key;
  Oid last_reader_id, last_writer_id;
  Timestamp last_read, last_write;
  AclMode perms;
} localAccessEntry;

typedef struct relnameCacheEntry {
  Oid relid;
  char relname[NAMEDATALEN];
} relnameCacheEntry;

typedef struct fileDumpEntry {
  Oid dbid;
  char *filename;
  FILE *file;
} fileDumpEntry;

static int32 relaccess_size;
static bool dump_on_overflow;
static bool is_enabled;
static relaccessGlobalData *data;
static HTAB *relaccesses;
static HTAB *local_access_entries = NULL;
static const int32 LOCAL_HTAB_SZ = 128;
static HTAB *relname_cache = NULL;
static const int32 RELCACHE_SZ = 16;
static const int32 FILE_CACHE_SZ = 16;
static int stmt_counter = 0;
static bool had_ht_overflow = false;

#define IS_POSTGRES_DB                                                         \
  (strcmp("postgres", get_database_name(MyDatabaseId)) == 0)

#define is_write(perms)                                                        \
  (((perms) & (ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE)) != 0)

#define is_read(perms) (!is_write(perms) && ((perms) & ACL_SELECT) != 0)

/*
 * Shmem request hook: request shared memory and LWLocks.
 * In PG16+, RequestAddinShmemSpace/RequestNamedLWLockTranche must be called
 * from shmem_request_hook, not directly from _PG_init().
 */
static void relaccess_shmem_request() {
  if (prev_shmem_request_hook)
    prev_shmem_request_hook();

  RequestNamedLWLockTranche("gp_relaccess_stats", 2);
  Size size = MAXALIGN(sizeof(relaccessGlobalData));
  size = add_size(size,
                  hash_estimate_size(relaccess_size, sizeof(relaccessEntry)));
  RequestAddinShmemSpace(size);
}

static void relaccess_shmem_startup() {
  bool found;
  HASHCTL info;

  if (prev_shmem_startup_hook)
    prev_shmem_startup_hook();

  LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

  data = (relaccessGlobalData *)(ShmemInitStruct(
      "relaccess_stats", sizeof(relaccessGlobalData), &found));
  if (!found) {
    LWLockPadded *locks = GetNamedLWLockTranche("gp_relaccess_stats");
    data->relaccess_ht_lock = &locks[0].lock;
    data->relaccess_file_lock = &locks[1].lock;
  }

  memset(&info, 0, sizeof(info));
  info.keysize = sizeof(relaccessHashKey);
  info.entrysize = sizeof(relaccessEntry);
  info.hash = relaccess_hash_fn;
  info.match = relaccess_match_fn;
  relaccesses = ShmemInitHash(
      "relaccess_stats hash", relaccess_size, relaccess_size, &info,
      (HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_FIXED_SIZE));

  LWLockRelease(AddinShmemInitLock);

  if (!IsUnderPostmaster) {
    on_shmem_exit(relaccess_shmem_shutdown, (Datum)0);
  }
}

static void relaccess_shmem_shutdown(int code, Datum arg) {
  if (code || !data || !relaccesses) {
    return;
  }
  LWLockAcquire(data->relaccess_ht_lock, LW_EXCLUSIVE);
  relaccess_dump_to_files(false);
  LWLockRelease(data->relaccess_ht_lock);
}

static uint32 relaccess_hash_fn(const void *key, Size keysize) {
  const relaccessHashKey *k = (const relaccessHashKey *)key;
  return hash_uint32((uint32)k->dbid) ^ hash_uint32((uint32)k->relid);
}

static int relaccess_match_fn(const void *key1, const void *key2,
                              Size keysize) {
  const relaccessHashKey *k1 = (const relaccessHashKey *)key1;
  const relaccessHashKey *k2 = (const relaccessHashKey *)key2;
  return (k1->dbid == k2->dbid && k1->relid == k2->relid) ? 0 : 1;
}

static uint32 local_relaccess_hash_fn(const void *key, Size keysize) {
  const localAccessKey *k = (const localAccessKey *)key;
  return hash_uint32((uint32)k->stmt_cnt) ^ hash_uint32((uint32)k->relid);
}

static int local_relaccess_match_fn(const void *key1, const void *key2,
                                    Size keysize) {
  const localAccessKey *k1 = (const localAccessKey *)key1;
  const localAccessKey *k2 = (const localAccessKey *)key2;
  return (k1->stmt_cnt == k2->stmt_cnt && k1->relid == k2->relid) ? 0 : 1;
}

void _PG_init(void) {
  if (!process_shared_preload_libraries_in_progress) {
    return;
  }

  DefineCustomIntVariable(
      "gp_relaccess_stats.max_tables",
      "Sets the maximum number of tables cached by gp_relaccess_stats.", NULL,
      &relaccess_size, 65536, 128, INT_MAX, PGC_POSTMASTER, 0, NULL, NULL,
      NULL);

  DefineCustomBoolVariable("gp_relaccess_stats.dump_on_overflow",
                           "Selects whether we should dump to .csv in case "
                           "gp_relaccess_stats.max_tables is exceeded.",
                           NULL, &dump_on_overflow, false, PGC_SIGHUP, 0, NULL,
                           NULL, NULL);

  DefineCustomBoolVariable(
      "gp_relaccess_stats.enabled",
      "Collect table access stats globally or for a specific database. "
      "Note that shared memory is initialized indepemdent of this argument.",
      NULL, &is_enabled, false, PGC_SUSET, 0, NULL, NULL, NULL);

  if (Gp_role != GP_ROLE_DISPATCH) {
    return;
  }

  prev_shmem_request_hook = shmem_request_hook;
  shmem_request_hook = relaccess_shmem_request;
  prev_shmem_startup_hook = shmem_startup_hook;
  shmem_startup_hook = relaccess_shmem_startup;
  prev_check_perms_hook = ExecutorCheckPerms_hook;
  ExecutorCheckPerms_hook = collect_relaccess_hook;
  next_ProcessUtility_hook = ProcessUtility_hook;
  ProcessUtility_hook = collect_truncate_hook;
  prev_ExecutorEnd_hook = ExecutorEnd_hook;
  ExecutorEnd_hook = relaccess_executor_end_hook;
  prev_object_access_hook = object_access_hook;
  object_access_hook = relaccess_drop_hook;
  RegisterXactCallback(relaccess_xact_callback, NULL);
  HASHCTL ctl;
  MemSet(&ctl, 0, sizeof(ctl));
  ctl.keysize = sizeof(localAccessKey);
  ctl.entrysize = sizeof(localAccessEntry);
  ctl.hash = local_relaccess_hash_fn;
  ctl.match = local_relaccess_match_fn;
  local_access_entries =
      hash_create("Transaction-wide relaccess entries", LOCAL_HTAB_SZ, &ctl,
                  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
  MemSet(&ctl, 0, sizeof(ctl));
  ctl.keysize = sizeof(Oid);
  ctl.entrysize = sizeof(relnameCacheEntry);
  ctl.hash = oid_hash;
  relname_cache = hash_create("Transaction-wide relation name cache",
                              RELCACHE_SZ, &ctl, HASH_ELEM | HASH_FUNCTION);
}

static bool collect_relaccess_hook(List *rangeTable, List *rtePermInfos,
                                   bool ereport_on_violation) {
  if (prev_check_perms_hook &&
      !prev_check_perms_hook(rangeTable, rtePermInfos, ereport_on_violation)) {
    return false;
  }
  if (Gp_role == GP_ROLE_DISPATCH && is_enabled) {
    ListCell *l;
    foreach (l, rtePermInfos) {
      RTEPermissionInfo *perminfo = (RTEPermissionInfo *)lfirst(l);
      AclMode requiredPerms = perminfo->requiredPerms;
      if (is_read(requiredPerms) || is_write(requiredPerms)) {
        memorize_local_access_entry(perminfo->relid, requiredPerms);
        update_relname_cache(perminfo->relid, NULL);
      }
    }
  }
  return true;
}

static void collect_truncate_hook(PlannedStmt *pstmt, const char *queryString,
                                  bool readOnlyTree,
                                  ProcessUtilityContext context,
                                  ParamListInfo params,
                                  QueryEnvironment *queryEnv,
                                  DestReceiver *dest, QueryCompletion *qc) {
  Node *parsetree = pstmt->utilityStmt;
  if (nodeTag(parsetree) == T_TruncateStmt && is_enabled &&
      Gp_role == GP_ROLE_DISPATCH) {
    TruncateStmt *stmt = (TruncateStmt *)parsetree;
    ListCell *cell;
    /**
     *  TODO: TRUNCATE may be called with ONLY option which limits it only to
     *the root partition. Otherwise it will truncate all child partitions. We
     *might wish to track the difference by explicitly adding records for each
     *truncated partition in the future if it proves useful
     **/
    foreach (cell, stmt->relations) {
      RangeVar *rv = lfirst(cell);
      Relation rel = table_openrv(rv, AccessExclusiveLock);
      Oid relid = rel->rd_id;
      table_close(rel, NoLock);
      memorize_local_access_entry(relid, ACL_TRUNCATE);
      update_relname_cache(relid, rv->relname);
    }
  }
  if (next_ProcessUtility_hook) {
    (*next_ProcessUtility_hook)(pstmt, queryString, readOnlyTree, context,
                                params, queryEnv, dest, qc);
  } else {
    standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
                            queryEnv, dest, qc);
  }
}

#define UPDATE_STAT(lowercase, uppercase)                                      \
  dst_entry->n_##lowercase += (src_entry->perms & ACL_##uppercase ? 1 : 0)

// if there is a better way to cleanup a postgres hashtable
// w/o recreating it, I didn't find it
#define CLEAR_HTAB(entryType, hmap, key_name)                                  \
  {                                                                            \
    HASH_SEQ_STATUS hash_seq;                                                  \
    entryType *src_entry;                                                      \
    hash_seq_init(&hash_seq, hmap);                                            \
    while ((src_entry = hash_seq_search(&hash_seq)) != NULL) {                 \
      bool found;                                                              \
      hash_search(hmap, &src_entry->key_name, HASH_REMOVE, &found);            \
      Assert(found);                                                           \
    }                                                                          \
  }

static void relaccess_xact_callback(XactEvent event, void *arg) {
  if (Gp_role != GP_ROLE_DISPATCH || !is_enabled) {
    return;
  }
  // TODO: add support for savepoint rollbacks
  Assert(GetCurrentTransactionNestLevel() == 1);
  if (event == XACT_EVENT_COMMIT) {
    HASH_SEQ_STATUS hash_seq;
    localAccessEntry *src_entry;
    hash_seq_init(&hash_seq, local_access_entries);
    LWLockAcquire(data->relaccess_ht_lock, LW_EXCLUSIVE);
    while ((src_entry = hash_seq_search(&hash_seq)) != NULL) {
      bool found;
      relaccessHashKey key;
      key.dbid = MyDatabaseId;
      key.relid = src_entry->key.relid;
      long n_access_records = hash_get_num_entries(relaccesses);
      relaccessEntry *dst_entry = NULL;
      Assert(n_access_records <= relaccess_size);
      if (n_access_records == relaccess_size) {
        // no room for new entries. Perhaps this relid is already being tracked?
        dst_entry =
            (relaccessEntry *)hash_search(relaccesses, &key, HASH_FIND, &found);
      } else {
        dst_entry = (relaccessEntry *)hash_search(relaccesses, &key,
                                                  HASH_ENTER_NULL, &found);
      }
      if (dst_entry || dump_on_overflow) {
        if (!dst_entry) {
          // we are out of shared memory and need to dump
          relaccess_dump_to_files(false);
          // we MUST have enough space now, unless we were unable to dump
          dst_entry = (relaccessEntry *)hash_search(relaccesses, &key,
                                                    HASH_ENTER_NULL, &found);
          if (!dst_entry) {
            // still no memory left
            if (!had_ht_overflow) {
              elog(WARNING, ("gp_relaccess_stats.max_tables is exceeded and we "
                             "are unable to dump hashtables to disk. "
                             "Will start loosing some relaccess stats"));
              had_ht_overflow = true;
            }
            continue;
          } else {
            had_ht_overflow = false;
          }
        }
        if (!found) {
          dst_entry->last_reader_id = InvalidOid;
          dst_entry->last_writer_id = InvalidOid;
          dst_entry->last_read = 0;
          dst_entry->last_write = 0;
          dst_entry->n_select = 0;
          dst_entry->n_insert = 0;
          dst_entry->n_update = 0;
          dst_entry->n_delete = 0;
          dst_entry->n_truncate = 0;
        }
        UPDATE_STAT(select, SELECT);
        UPDATE_STAT(insert, INSERT);
        UPDATE_STAT(update, UPDATE);
        UPDATE_STAT(delete, DELETE);
        UPDATE_STAT(truncate, TRUNCATE);
        if (src_entry->last_read > dst_entry->last_read) {
          dst_entry->last_read = src_entry->last_read;
          dst_entry->last_reader_id = src_entry->last_reader_id;
        }
        if (src_entry->last_write > dst_entry->last_write) {
          dst_entry->last_write = src_entry->last_write;
          dst_entry->last_writer_id = src_entry->last_writer_id;
        }
        relnameCacheEntry *namecache_entry = (relnameCacheEntry *)hash_search(
            relname_cache, &key.relid, HASH_ENTER, &found);
        Assert(namecache_entry);
        strlcpy(dst_entry->relname, namecache_entry->relname,
                sizeof(dst_entry->relname));
      } else {
        if (!had_ht_overflow) {
          elog(WARNING, "gp_relaccess_stats.max_tables is exceeded! New table "
                        "events will be lost. "
                        "Please execute relaccess_stats_update() and consider "
                        "setting a hihger value");
        }
        had_ht_overflow = true;
      }
    }
    LWLockRelease(data->relaccess_ht_lock);
    CLEAR_HTAB(localAccessEntry, local_access_entries, key);
    CLEAR_HTAB(relnameCacheEntry, relname_cache, relid);
  } else if (event == XACT_EVENT_ABORT) {
    CLEAR_HTAB(localAccessEntry, local_access_entries, key);
    CLEAR_HTAB(relnameCacheEntry, relname_cache, relid);
  }
}

Datum relaccess_stats_update(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;

  if (SRF_IS_FIRSTCALL()) {
    funcctx = SRF_FIRSTCALL_INIT();
    funcctx->max_calls = 1;
    relaccess_stats_update_internal();
  }

  funcctx = SRF_PERCALL_SETUP();
  if (funcctx->call_cntr < funcctx->max_calls) {
    SRF_RETURN_NEXT(funcctx, (Datum)0);
  }
  SRF_RETURN_DONE(funcctx);
}

Datum relaccess_stats_dump(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;

  if (SRF_IS_FIRSTCALL()) {
    funcctx = SRF_FIRSTCALL_INIT();
    funcctx->max_calls = 1;
    LWLockAcquire(data->relaccess_ht_lock, LW_EXCLUSIVE);
    relaccess_dump_to_files(true);
    LWLockRelease(data->relaccess_ht_lock);
  }

  funcctx = SRF_PERCALL_SETUP();
  if (funcctx->call_cntr < funcctx->max_calls) {
    SRF_RETURN_NEXT(funcctx, (Datum)0);
  }
  SRF_RETURN_DONE(funcctx);
}

Datum relaccess_stats_fillfactor(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;

  if (SRF_IS_FIRSTCALL()) {
    int16_t fillfactor;

    funcctx = SRF_FIRSTCALL_INIT();
    funcctx->max_calls = 1;
    LWLockAcquire(data->relaccess_ht_lock, LW_SHARED);
    fillfactor = hash_get_num_entries(relaccesses) * 100 / relaccess_size;
    LWLockRelease(data->relaccess_ht_lock);
    funcctx->user_fctx = (void *)(intptr_t)fillfactor;
  }

  funcctx = SRF_PERCALL_SETUP();
  if (funcctx->call_cntr < funcctx->max_calls) {
    SRF_RETURN_NEXT(funcctx,
                    Int16GetDatum((int16_t)(intptr_t)funcctx->user_fctx));
  }
  SRF_RETURN_DONE(funcctx);
}

Datum relaccess_stats_from_dump(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;
  List *stats_entries = NIL;

  if (SRF_IS_FIRSTCALL()) {
    funcctx = SRF_FIRSTCALL_INIT();
    MemoryContext oldcontext =
        MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
    TupleDesc tupdesc = CreateTemplateTupleDesc(11);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "relid", OIDOID, -1 /* typmod */,
                       0 /* attdim */);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "relname", NAMEOID,
                       -1 /* typmod */, 0 /* attdim */);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "last_reader_id", OIDOID,
                       -1 /* typmod */, 0 /* attdim */);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "last_writer_id", OIDOID,
                       -1 /* typmod */, 0 /* attdim */);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "last_read", TIMESTAMPTZOID,
                       -1 /* typmod */, 0 /* attdim */);
    TupleDescInitEntry(tupdesc, (AttrNumber)6, "last_write", TIMESTAMPTZOID,
                       -1 /* typmod */, 0 /* attdim */);
    TupleDescInitEntry(tupdesc, (AttrNumber)7, "n_select_queries", INT8OID,
                       -1 /* typmod */, 0 /* attdim */);
    TupleDescInitEntry(tupdesc, (AttrNumber)8, "n_insert_queries", INT8OID,
                       -1 /* typmod */, 0 /* attdim */);
    TupleDescInitEntry(tupdesc, (AttrNumber)9, "n_update_queries", INT8OID,
                       -1 /* typmod */, 0 /* attdim */);
    TupleDescInitEntry(tupdesc, (AttrNumber)10, "n_delete_queries", INT8OID,
                       -1 /* typmod */, 0 /* attdim */);
    TupleDescInitEntry(tupdesc, (AttrNumber)11, "n_truncate_queries", INT8OID,
                       -1 /* typmod */, 0 /* attdim */);
    funcctx->tuple_desc = BlessTupleDesc(tupdesc);
    StringInfoData dump_file = get_dump_filename(MyDatabaseId);
    FILE *dump = AllocateFile(dump_file.data, "rb");
    pfree(dump_file.data);
    if (dump) {
      while (true) {
        relaccessEntry *entry = palloc(sizeof(relaccessEntry));
        if (fread(entry, sizeof(relaccessEntry), 1, dump) != 1) {
          pfree(entry);
          break;
        }
        stats_entries = lappend(stats_entries, entry);
      }
      FreeFile(dump);
    }
    funcctx->user_fctx = stats_entries;
    MemoryContextSwitchTo(oldcontext);
  }

  funcctx = SRF_PERCALL_SETUP();
  stats_entries = (List *)funcctx->user_fctx;

  while (true) {
    if (stats_entries == NIL) {
      SRF_RETURN_DONE(funcctx);
    }
    relaccessEntry *entry = linitial(stats_entries);
    stats_entries = list_delete_first(stats_entries);
    Datum values[11];
    bool nulls[11];
    MemSet(nulls, 0, sizeof(nulls));
    values[0] = ObjectIdGetDatum(entry->key.relid);
    values[1] = CStringGetDatum(entry->relname);
    values[2] = ObjectIdGetDatum(entry->last_reader_id);
    values[3] = ObjectIdGetDatum(entry->last_writer_id);
    values[4] = TimestampTzGetDatum(entry->last_read);
    values[5] = TimestampTzGetDatum(entry->last_write);
    values[6] = Int64GetDatum(entry->n_select);
    values[7] = Int64GetDatum(entry->n_insert);
    values[8] = Int64GetDatum(entry->n_update);
    values[9] = Int64GetDatum(entry->n_delete);
    values[10] = Int64GetDatum(entry->n_truncate);
    HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    Datum result = HeapTupleGetDatum(tuple);
    funcctx->user_fctx = stats_entries;
    /** NOTE: Cannot delete entry from this iteration right now.
     * For now let's rely on multi_call_memory_ctx until there is a proven
     * memory problem with this codepath
     */
    // pfree(entry);
    SRF_RETURN_NEXT(funcctx, result);
  }
}

static void relaccess_stats_update_internal() {
  LWLockAcquire(data->relaccess_ht_lock, LW_EXCLUSIVE);
  relaccess_dump_to_files(true);
  LWLockRelease(data->relaccess_ht_lock);
  relaccess_upsert_from_file();
}

static void add_file_dump_entry(Oid dbid, HTAB *ht) {
  bool found;
  fileDumpEntry *file_entry = hash_search(ht, &dbid, HASH_ENTER, &found);
  if (!found) {
    file_entry->dbid = dbid;
    StringInfoData filename = get_dump_filename(file_entry->dbid);
    file_entry->filename = filename.data;
    file_entry->file = AllocateFile(file_entry->filename, "ab");
  }
}

static void relaccess_dump_to_files(bool only_this_db) {
  HTAB *file_mapping;
  HASHCTL ctl;
  MemSet(&ctl, 0, sizeof(ctl));
  ctl.keysize = sizeof(Oid);
  ctl.entrysize = sizeof(fileDumpEntry);
  ctl.hash = oid_hash;
  file_mapping = hash_create("Relaccess dump files", FILE_CACHE_SZ, &ctl,
                             HASH_ELEM | HASH_FUNCTION);
  LWLockAcquire(data->relaccess_file_lock, LW_EXCLUSIVE);
  if (only_this_db) {
    add_file_dump_entry(MyDatabaseId, file_mapping);
  } else {
    HASH_SEQ_STATUS hash_seq;
    relaccessEntry *access_entry;
    hash_seq_init(&hash_seq, relaccesses);
    while ((access_entry = hash_seq_search(&hash_seq)) != NULL) {
      add_file_dump_entry(access_entry->key.dbid, file_mapping);
    }
  }
  relaccess_dump_to_files_internal(file_mapping);
  HASH_SEQ_STATUS hash_seq;
  hash_seq_init(&hash_seq, file_mapping);
  fileDumpEntry *entry;
  while ((entry = hash_seq_search(&hash_seq)) != NULL) {
    FreeFile(entry->file);
    pfree(entry->filename);
  }
  LWLockRelease(data->relaccess_file_lock);
  hash_destroy(file_mapping);
}

static void relaccess_dump_to_files_internal(HTAB *files) {
  HASH_SEQ_STATUS hash_seq;
  relaccessEntry *entry;
  hash_seq_init(&hash_seq, relaccesses);
  while ((entry = hash_seq_search(&hash_seq)) != NULL) {
    bool found;
    fileDumpEntry *dumpfile =
        hash_search(files, &entry->key.dbid, HASH_FIND, &found);
    if (!found) {
      // we don't want to dump events from this DB
      continue;
    }
    if (fwrite(entry, sizeof(relaccessEntry), 1, dumpfile->file) != 1) {
      hash_seq_term(&hash_seq);
      ereport(WARNING,
              (errcode_for_file_access(),
               errmsg("could not write gp_relaccess_stats file \"%s\": %m",
                      dumpfile->filename)));
      break;
    }
    hash_search(relaccesses, &entry->key, HASH_REMOVE, &found);
    had_ht_overflow = false;
  }
}

static void relaccess_upsert_from_file() {
  int ret;
  if ((ret = SPI_connect()) < 0) {
    elog(ERROR, "SPI connect failure - returned %d", ret);
  }
  LWLockAcquire(data->relaccess_file_lock, LW_EXCLUSIVE);
  StringInfoData filename = get_dump_filename(MyDatabaseId);
  StringInfoData query;
  initStringInfo(&query);
  appendStringInfo(&query,
                   "SELECT relaccess.__relaccess_upsert_from_dump_file()");
  ret = SPI_execute(query.data, false, 1);
  unlink(filename.data);
  LWLockRelease(data->relaccess_file_lock);
  SPI_finish();
  if (ret < 0) {
    elog(ERROR, "SPI execute failure - returned %d", ret);
  }
}

static void update_relname_cache(Oid relid, char *relname) {
  bool found;
  relnameCacheEntry *relname_entry = (relnameCacheEntry *)hash_search(
      relname_cache, &relid, HASH_ENTER, &found);
  if (!found) {
    relname_entry->relid = relid;
    if (!relname) {
      strlcpy(relname_entry->relname, get_rel_name(relid),
              sizeof(relname_entry->relname));
    } else {
      strlcpy(relname_entry->relname, relname, sizeof(relname_entry->relname));
    }
  } else {
    /**
     * NOTE: as we don't handle the 'else' clause here, there will be cases when
     * we write outdated table names, like below:
     *    BEGIN;
     *      INSERT INTO tbl VALUES (1);
     *      ALTER TABLE tbl RENAME TO new_tbl;
     *      SELECT * FROM new_tbl;
     *    COMMIT;
     * In this case both INSERT and SELECT stmts would be counted with the
     * old'tbl' name, as we don't update our cache for already known relids in
     * the same transaction. This is a deliberate decision for performance
     * reasons.
     */
  }
}

static void memorize_local_access_entry(Oid relid, AclMode perms) {
  bool found;
  localAccessKey key;
  key.stmt_cnt = stmt_counter;
  key.relid = relid;
  localAccessEntry *entry = (localAccessEntry *)hash_search(
      local_access_entries, &key, HASH_ENTER, &found);
  if (!found) {
    entry->last_read = entry->last_write = InvalidOid;
    entry->perms = perms;
    entry->last_read = 0;
    entry->last_write = 0;
  } else {
    entry->perms |= perms;
  }
  TimestampTz curts = GetCurrentTimestamp();
  if (is_read(perms)) {
    entry->last_reader_id = GetUserId();
    entry->last_read = curts;
  }
  if (is_write(perms)) {
    entry->last_writer_id = GetUserId();
    entry->last_write = curts;
  }
}

static void relaccess_executor_end_hook(QueryDesc *query_desc) {
  if (prev_ExecutorEnd_hook) {
    prev_ExecutorEnd_hook(query_desc);
  } else {
    standard_ExecutorEnd(query_desc);
  }
  // Unfortunately, we cannot safely rely on gp_command_counter as
  // it is being incremented more than once for many statements.
  // So we have to maintain our own statement counter.
  stmt_counter++;
}

static StringInfoData get_dump_filename(Oid dbid) {
  StringInfoData filename;
  initStringInfoOfSize(&filename, 256);
  appendStringInfo(&filename, "%s/relaccess_stats_dump_%d.csv",
                   PGSTAT_STAT_PERMANENT_DIRECTORY, dbid);
  return filename;
}

static void relaccess_drop_hook(ObjectAccessType access, Oid classId,
                                Oid objectId, int subId, void *arg) {
  if (prev_object_access_hook) {
    prev_object_access_hook(access, classId, objectId, subId, arg);
  }
  // we don't want shared memory and .csv files hanging around forever
  // for databases that we've dropped.
  // This function cleans up both files and shmem
  if (classId == DatabaseRelationId && access == OAT_DROP) {
    LWLockAcquire(data->relaccess_ht_lock, LW_EXCLUSIVE);
    HASH_SEQ_STATUS hash_seq;
    relaccessEntry *entry;
    hash_seq_init(&hash_seq, relaccesses);
    while ((entry = hash_seq_search(&hash_seq)) != NULL) {
      if (entry->key.dbid == objectId) {
        bool found;
        hash_search(relaccesses, &entry->key, HASH_REMOVE, &found);
        had_ht_overflow = false;
      }
    }
    LWLockRelease(data->relaccess_ht_lock);
    LWLockAcquire(data->relaccess_file_lock, LW_EXCLUSIVE);
    StringInfoData filename = get_dump_filename(objectId);
    unlink(filename.data);
    pfree(filename.data);
    LWLockRelease(data->relaccess_file_lock);
  }
}
