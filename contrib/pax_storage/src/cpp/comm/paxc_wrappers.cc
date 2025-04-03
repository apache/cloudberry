/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * paxc_wrappers.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/paxc_wrappers.cc
 *
 *-------------------------------------------------------------------------
 */

#include "comm/paxc_wrappers.h"

#include "comm/cbdb_api.h"

#include <sys/stat.h>
#include <unistd.h>

#include "storage/wal/paxc_wal.h"

#define PAX_MICROPARTITION_NAME_LENGTH 2048
#define PAX_MICROPARTITION_DIR_POSTFIX "_pax"

namespace paxc {
// pax file operation
static void DeletePaxDirectoryPathRecursive(
    const char *path, const char *toplevel_path, bool delete_topleveldir,
    void (*action)(const char *fname, bool isdir, int elevel),
    bool process_symlinks, int elevel);

static void UnlinkIfExistsFname(const char *fname, bool isdir, int elevel);

// MakedirRecursive: function used to create directory recursively by a
// specified directory path. parameter path IN directory path. return void.
void MakedirRecursive(const char *path) {
  char dirpath[PAX_MICROPARTITION_NAME_LENGTH];
  unsigned int pathlen = strlen(path);
  struct stat st {};

  Assert(path != NULL && path[0] != '\0' &&
         pathlen < PAX_MICROPARTITION_NAME_LENGTH);

  for (unsigned int i = 0; i <= pathlen; i++) {
    if (path[i] == '/' || path[i] == '\0') {
      strncpy(dirpath, path, i + 1);
      dirpath[i + 1] = '\0';
      if (stat(dirpath, &st) != 0) {
        if (MakePGDirectory(dirpath) != 0)
          ereport(
              ERROR,
              (errcode_for_file_access(),
               errmsg("MakedirRecursive could not create directory \"%s\": %m",
                      dirpath)));
      }
    }
  }
}

// DeletePaxDirectoryPath: Delete a directory and everything in it, if it
// exists. parameter dirname IN directory to delete recursively. parameter
// reserve_topdir IN flag indicate if reserve top level directory.
void DeletePaxDirectoryPath(const char *dirname, bool delete_topleveldir) {
  struct stat statbuf {};

  if (stat(dirname, &statbuf) != 0) {
    // Silently ignore missing directory.
    if (errno == ENOENT)
      return;
    else
      ereport(
          ERROR,
          (errcode_for_file_access(),
           errmsg("Check PAX file directory failed, directory path: \"%s\": %m",
                  dirname)));
  }

  DeletePaxDirectoryPathRecursive(dirname, dirname, delete_topleveldir,
                                  UnlinkIfExistsFname, false, LOG);
}

// BuildPaxDirectoryPath: function used to build pax storage directory path
// following pg convension, for example base/{database_oid}/{blocks_relid}_pax.
// parameter rd_node IN relfilenode information.
// parameter rd_backend IN backend transaction id.
// return palloc'd pax storage directory path.
char *BuildPaxDirectoryPath(RelFileNode rd_node, BackendId rd_backend) {
  char *relpath = NULL;
  char *paxrelpath = NULL;
  relpath = relpathbackend(rd_node, rd_backend, MAIN_FORKNUM);
  Assert(relpath[0] != '\0');
  paxrelpath = psprintf("%s%s", relpath, PAX_MICROPARTITION_DIR_POSTFIX);
  pfree(relpath);
  return paxrelpath;
}

static void UnlinkIfExistsFname(const char *fname, bool isdir, int elevel) {
  if (isdir) {
    if (rmdir(fname) != 0 && errno != ENOENT)
      ereport(elevel, (errcode_for_file_access(),
                       errmsg("could not remove directory \"%s\": %m", fname)));
  } else {
    // Use PathNameDeleteTemporaryFile to report filesize
    PathNameDeleteTemporaryFile(fname, false);
  }
}

static void DeletePaxDirectoryPathRecursive(
    const char *path, const char *toplevel_path, bool delete_topleveldir,
    void (*action)(const char *fname, bool isdir, int elevel),
    bool process_symlinks, int elevel) {
  DIR *dir;
  struct dirent *de;
  dir = AllocateDir(path);

  while ((de = ReadDirExtended(dir, path, elevel)) != NULL) {
    char subpath[MAXPGPATH * 2];
    CHECK_FOR_INTERRUPTS();

    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;

    snprintf(subpath, sizeof(subpath), "%s/%s", path, de->d_name);

    switch (get_dirent_type(subpath, de, process_symlinks, elevel)) {
      case PGFILETYPE_REG:
        (*action)(subpath, false, elevel);
        break;
      case PGFILETYPE_DIR:
        DeletePaxDirectoryPathRecursive(
            subpath, toplevel_path, delete_topleveldir, action, false, elevel);
        break;
      default:
        break;
    }
  }

  // ignore any error here for delete
  FreeDir(dir);

  // skip deleting top level dir if delete_topleveldir is set to false.
  if (delete_topleveldir || strncmp(path, toplevel_path, strlen(path)) != 0) {
    //  it's important to fsync the destination directory itself as individual
    //  file fsyncs don't guarantee that the directory entry for the file is
    //  synced.  However, skip this if AllocateDir failed; the action function
    //  might not be robust against that.

    if (dir) (*action)(path, true, elevel);
  }
}

bool PGGetOperator(const char *operatorName, Oid operatorNamespace,
                   Oid leftObjectId, Oid rightObjectId, Oid *opno,
                   FmgrInfo *finfo) {
  HeapTuple tup;
  FmgrInfo dummy;

  if (!operatorName) {
    goto failed;
  }

  tup = SearchSysCache4(OPERNAMENSP, PointerGetDatum(operatorName),
                        ObjectIdGetDatum(leftObjectId),
                        ObjectIdGetDatum(rightObjectId),
                        ObjectIdGetDatum(operatorNamespace));
  if (HeapTupleIsValid(tup)) {
    Form_pg_operator oprform = (Form_pg_operator)GETSTRUCT(tup);

    *opno = oprform->oid;
    fmgr_info_cxt(oprform->oprcode, finfo ? finfo : &dummy,
                  CurrentMemoryContext);
    ReleaseSysCache(tup);
    return true;
  }

failed:
  *opno = InvalidOid;
  return false;
}

bool PGGetOperatorNo(Oid opno, NameData *oprname, Oid *oprleft, Oid *oprright,
                     FmgrInfo *finfo) {
  HeapTuple tup;
  Form_pg_operator op;
  FmgrInfo dummy;

  tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
  // should not happen
  if (!HeapTupleIsValid(tup))
    elog(ERROR, "cache lookup failed for operator %u", opno);
  op = (Form_pg_operator)GETSTRUCT(tup);
  *oprname = op->oprname;
  *oprleft = op->oprleft;
  *oprright = op->oprright;

  fmgr_info_cxt(op->oprcode, finfo ? finfo : &dummy, CurrentMemoryContext);
  ReleaseSysCache(tup);

  return true;
}

bool PGGetAddOperator(Oid atttypid, Oid subtype, Oid namespc, Oid *resulttype,
                      FmgrInfo *finfo) {
  static const char *oprname = "+";
  FmgrInfo dummy;
  HeapTuple tuple;
  Oid oprcode;
  bool is_null;

  tuple = SearchSysCache4(OPERNAMENSP, PointerGetDatum(oprname),
                          ObjectIdGetDatum(atttypid), ObjectIdGetDatum(subtype),
                          ObjectIdGetDatum(namespc));
  if (!HeapTupleIsValid(tuple)) {
    // not found add function in pg_operator
    return false;
  }

  oprcode = DatumGetObjectId(
      SysCacheGetAttr(OPERNAMENSP, tuple, Anum_pg_operator_oprcode, &is_null));
  Assert(!is_null && RegProcedureIsValid(oprcode));

  *resulttype = DatumGetObjectId(SysCacheGetAttr(
      OPERNAMENSP, tuple, Anum_pg_operator_oprresult, &is_null));

  ReleaseSysCache(tuple);

  fmgr_info_cxt(oprcode, finfo ? finfo : &dummy, CurrentMemoryContext);

  return true;
}

bool PGGetProc(Oid procoid, FmgrInfo *finfo) {
  HeapTuple tuple;
  bool is_null;
  FmgrInfo dummy;
  Datum prosrc_datum;
  char *prosrc;
  Oid func_oid;

  /*
   * We do not honor check_function_bodies since it's unlikely the function
   * name will be found later if it isn't there now.
   */

  tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(procoid));
  if (!HeapTupleIsValid(tuple)) return false;

  prosrc_datum = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_prosrc, &is_null);
  if (is_null) return false;
  prosrc = TextDatumGetCString(prosrc_datum);
  func_oid = fmgr_internal_function(prosrc);
  if (func_oid == InvalidOid)
    ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
             errmsg("there is no built-in function named \"%s\"", prosrc)));

  fmgr_info_cxt(func_oid, finfo ? finfo : &dummy, CurrentMemoryContext);

  ReleaseSysCache(tuple);

  return true;
}

// Used to get the agg info
// select * from pg_aggregate as a left join pg_proc as b on a.aggfnoid=b.oid
// where b.proname='procedure' and b.proargtypes = 'atttypid';
bool PGGetAggInfo(const char *procedure, Oid atttypid, Oid *prorettype,
                  Oid *transtype, FmgrInfo *trans_finfo, FmgrInfo *final_finfo,
                  bool *final_func_exist, bool *agginitval_isnull) {
  Oid proc_oid;
  HeapTuple tuple, agg_tuple;
  Datum agg_transfn, agg_finalfn;
  FmgrInfo dummy;
  bool is_null;

  auto oid_vec = buildoidvector(&atttypid, 1);

  // 1. open the pg_proc get the `prorettype` and `funcid`
  tuple = SearchSysCache3(PROCNAMEARGSNSP, PointerGetDatum(procedure),
                          PointerGetDatum(oid_vec),
                          ObjectIdGetDatum(PG_CATALOG_NAMESPACE));
  pfree(oid_vec);

  if (!HeapTupleIsValid(tuple)) {
    // not found sum function in pg_proc
    return false;
  }

  *prorettype = DatumGetObjectId(SysCacheGetAttr(
      PROCNAMEARGSNSP, tuple, Anum_pg_proc_prorettype, &is_null));
  Assert(!is_null && RegProcedureIsValid(*prorettype));

  proc_oid = DatumGetObjectId(
      SysCacheGetAttr(PROCNAMEARGSNSP, tuple, Anum_pg_proc_oid, &is_null));
  Assert(!is_null && OidIsValid(proc_oid));

  ReleaseSysCache(tuple);

  // 2. open the pg_aggregate get the `agg_transfn`/`agg_finalfn`(if
  // exist) and check the `init_val` is null
  agg_tuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(proc_oid));
  if (unlikely(!HeapTupleIsValid(agg_tuple))) {
    // catalog not macth, should not happend
    Assert(false);
    return false;
  }

  agg_transfn = SysCacheGetAttr(AGGFNOID, agg_tuple,
                                Anum_pg_aggregate_aggtransfn, &is_null);
  Assert(!is_null);
  if (!RegProcedureIsValid(agg_transfn)) {
    ReleaseSysCache(agg_tuple);
    // found sum function but not found transfn in pg_agg
    return false;
  }

  // final function may not exist
  agg_finalfn = SysCacheGetAttr(AGGFNOID, agg_tuple,
                                Anum_pg_aggregate_aggfinalfn, &is_null);
  *final_func_exist = !is_null && RegProcedureIsValid(agg_finalfn);

  *transtype = SysCacheGetAttr(AGGFNOID, agg_tuple,
                               Anum_pg_aggregate_aggtranstype, &is_null);
  Assert(!is_null && RegProcedureIsValid(*transtype));

  SysCacheGetAttr(AGGFNOID, agg_tuple, Anum_pg_aggregate_agginitval, &is_null);
  *agginitval_isnull = is_null;

  ReleaseSysCache(agg_tuple);

  // 3. create the `trans_finfo`/final_finfo(if exist)/`add_finfo`
  fmgr_info_cxt(agg_transfn, trans_finfo ? trans_finfo : &dummy,
                CurrentMemoryContext);
  if (*final_func_exist) {
    fmgr_info_cxt(agg_finalfn, final_finfo ? final_finfo : &dummy,
                  CurrentMemoryContext);
  }

  return true;
}

bool SumAGGGetProcinfo(Oid atttypid, Oid *prorettype, Oid *transtype,
                       FmgrInfo *trans_finfo, FmgrInfo *final_finfo,
                       bool *final_func_exist, FmgrInfo *add_finfo) {
  static const char *procedure_sum = "sum";
  FmgrInfo dummy;
  HeapTuple tuple, agg_tuple;
  Oid funcid;
  Oid addrettyp;
  Datum agg_transfn;
  Datum agg_finalfn;
  bool is_null;

  // 1. open the pg_proc get the `prorettype` and `funcid`
  auto oid_vec = buildoidvector(&atttypid, 1);
  tuple = SearchSysCache3(PROCNAMEARGSNSP, PointerGetDatum(procedure_sum),
                          PointerGetDatum(oid_vec),
                          ObjectIdGetDatum(PG_CATALOG_NAMESPACE));
  pfree(oid_vec);
  if (!HeapTupleIsValid(tuple)) {
    // not found sum function in pg_proc
    return false;
  }

  *prorettype = DatumGetObjectId(SysCacheGetAttr(
      PROCNAMEARGSNSP, tuple, Anum_pg_proc_prorettype, &is_null));
  Assert(!is_null && RegProcedureIsValid(*prorettype));

  funcid = DatumGetObjectId(
      SysCacheGetAttr(PROCNAMEARGSNSP, tuple, Anum_pg_proc_oid, &is_null));
  Assert(!is_null);
  ReleaseSysCache(tuple);

  // 2. open the pg_operator get the `add_func` which is `+(prorettype,
  // prorettype)`
  is_null = !PGGetAddOperator(*prorettype, *prorettype, PG_CATALOG_NAMESPACE,
                              &addrettyp, add_finfo);
  if (is_null || addrettyp != *prorettype) {
    // can't get the `add` operator or return type not match
    return false;
  }

  // 3. open the pg_aggregate get the `agg_transfn`/`agg_finalfn`(if
  // exist) and check the `init_val` is null
  agg_tuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(funcid));
  if (unlikely(!HeapTupleIsValid(agg_tuple))) {
    // catalog not macth, should not happend
    Assert(false);
    return false;
  }

  agg_transfn = SysCacheGetAttr(AGGFNOID, agg_tuple,
                                Anum_pg_aggregate_aggtransfn, &is_null);
  Assert(!is_null);
  if (!RegProcedureIsValid(agg_transfn)) {
    ReleaseSysCache(agg_tuple);
    // found sum function but not found transfn in pg_agg
    return false;
  }

  // final function may not exist
  agg_finalfn = SysCacheGetAttr(AGGFNOID, agg_tuple,
                                Anum_pg_aggregate_aggfinalfn, &is_null);
  *final_func_exist = !is_null && RegProcedureIsValid(agg_finalfn);

  *transtype = SysCacheGetAttr(AGGFNOID, agg_tuple,
                               Anum_pg_aggregate_aggtranstype, &is_null);
  Assert(!is_null && RegProcedureIsValid(*transtype));

  SysCacheGetAttr(AGGFNOID, agg_tuple, Anum_pg_aggregate_agginitval, &is_null);
  // sum agg must have not the init value
  // will use the transtype to init the trans val
  Assert(is_null);

  ReleaseSysCache(agg_tuple);

  // 4. create the `trans_finfo`/final_finfo(if exist)/`add_finfo`
  fmgr_info_cxt(agg_transfn, trans_finfo ? trans_finfo : &dummy,
                CurrentMemoryContext);
  if (*final_func_exist) {
    fmgr_info_cxt(agg_finalfn, final_finfo ? final_finfo : &dummy,
                  CurrentMemoryContext);
  }

  return true;
}

Datum SumFuncCall(FmgrInfo *flinfo, AggState *state, Datum arg1, Datum arg2) {
  LOCAL_FCINFO(fcinfo, 2);
  Datum result;

  InitFunctionCallInfoData(*fcinfo, flinfo, 2, InvalidOid, (Node *)state, NULL);

  fcinfo->args[0].value = arg1;
  fcinfo->args[0].isnull = false;
  fcinfo->args[1].value = arg2;
  fcinfo->args[1].isnull = false;

  result = FunctionCallInvoke(fcinfo);

  /* Check for null result, since caller is clearly not expecting one */
  if (fcinfo->isnull) elog(ERROR, "function %u returned NULL", flinfo->fn_oid);

  return result;
}

// can't use dfs-tablespace-ext directly, copy it
bool IsDfsTablespaceById(Oid spcId) {
  if (spcId == InvalidOid) return false;

  TableSpaceCacheEntry *spc = get_tablespace(spcId);
  return spc->opts && (spc->opts->pathOffset != 0);
}

bool NeedWAL(Relation rel) {
  return rel->rd_rel->relpersistence == RELPERSISTENCE_PERMANENT;
}

/**
 * @brief Create a directory for pax storage.
 * 1. Add pending delete for relfilenode/pax directory if abort transaction
 * 2. Add XLOG to create relfilenode/pax directory
 * 3. create relfilenode/pax directory, i.e. data directory.
 *     This function will do following this function call
 */
SMgrRelation PaxRelationCreateStorage(RelFileNode rnode, Relation rel)
{
  if (rel->rd_rel->relpersistence == RELPERSISTENCE_PERMANENT ||
      rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED) {
    paxc::XLogPaxCreateDirectory(rnode);
  }
  return RelationCreateStorage(rnode, rel->rd_rel->relpersistence, SMGR_PAX, rel);
}

}  // namespace paxc
