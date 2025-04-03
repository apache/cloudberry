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
 * paxc_wal.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/wal/paxc_wal.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/wal/paxc_wal.h"

#include "comm/cbdb_api.h"

#include <string.h>
#include <sys/stat.h>

#include "comm/guc.h"
#include "comm/log.h"
#include "comm/paxc_wrappers.h"
#include "storage/paxc_define.h"
#include "storage/wal/paxc_desc.h"

extern bool ignore_invalid_pages;
namespace paxc {

typedef struct xl_invalid_pax_file_key {
  RelFileNode node; /* the relation */
  char filename[MAX_PATH_FILE_NAME_LEN];
} xl_invalid_pax_file_key;

typedef struct xl_invalid_pax_file {
  xl_invalid_pax_file_key key;
} xl_invalid_pax_file;

static HTAB *invalid_pax_file_tab = NULL;

/*
 * Hash functions must have this signature.
 */
uint32 pax_file_key_hash(const void *key, Size keysize) {
  xl_invalid_pax_file_key *pax_file_key = (xl_invalid_pax_file_key *)key;
  return hash_any((const unsigned char *)key,
                  sizeof(RelFileNode) + strlen(pax_file_key->filename));
}

/*
 * Compare two pax file keys.
 * if match return 0, else return non-zero
 */
int pax_file_key_compare(const void *key1, const void *key2, Size keysize) {
  xl_invalid_pax_file_key *pax_file_key1 = (xl_invalid_pax_file_key *)key1;
  xl_invalid_pax_file_key *pax_file_key2 = (xl_invalid_pax_file_key *)key2;

  return !(RelFileNodeEquals(pax_file_key1->node, pax_file_key2->node) &&
           strncmp(pax_file_key1->filename, pax_file_key2->filename,
                   MAX_PATH_FILE_NAME_LEN) == 0);
}

static void LogInvalidPaxFile(RelFileNode node, const char *filename) {
  xl_invalid_pax_file_key key;
  bool found;

  if (invalid_pax_file_tab == NULL) {
    /* create hash table when first needed */
    HASHCTL ctl;

    memset(&ctl, 0, sizeof(ctl));

    ctl.keysize = sizeof(xl_invalid_pax_file_key);
    ctl.entrysize = sizeof(xl_invalid_pax_file);
    ctl.hash = pax_file_key_hash;
    ctl.match = pax_file_key_compare;

    invalid_pax_file_tab =
        hash_create("XLOG invalid-pax-file table", 100, &ctl,
                    HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
  }

  /* we currently assume xl_invalid_page_key contains no padding */
  key.node = node;
  strlcpy(key.filename, filename, MAX_PATH_FILE_NAME_LEN);
  hash_search(invalid_pax_file_tab, (void *)&key, HASH_ENTER, &found);
}

void LogInvalidPaxDirctory(RelFileNode node) {
  // mark the directory as invalid, filename is empty
  LogInvalidPaxFile(node, "");
}

bool IsPaxDirectoryValid(RelFileNode node) {
  xl_invalid_pax_file hentry;

  if (invalid_pax_file_tab == NULL) return true; /* nothing to do */

  hentry.key.node = node;
  strlcpy(hentry.key.filename, "", MAX_PATH_FILE_NAME_LEN);

  bool found = false;
  hash_search(invalid_pax_file_tab, (void *)&hentry.key, HASH_FIND, &found);
  return !found;
}

void XLogForgetInvalidPaxFile(RelFileNode node, const char *filename) {
  xl_invalid_pax_file hentry;

  if (invalid_pax_file_tab == NULL) return; /* nothing to do */

  hentry.key.node = node;
  strlcpy(hentry.key.filename, filename, MAX_PATH_FILE_NAME_LEN);

  bool found = false;
  hash_search(invalid_pax_file_tab, (void *)&hentry.key, HASH_REMOVE, &found);
  if (found) {
    PAX_LOG_IF(pax::pax_enable_debug,
               "forget invalid pax file, node: %u/%u/%u, filename: %s",
               node.dbNode, node.spcNode, node.relNode, filename);
  } else {
    PAX_LOG_IF(
        pax::pax_enable_debug,
        "forget invalid pax file not found, node: %u/%u/%u, filename: %s",
        node.dbNode, node.spcNode, node.relNode, filename);
  }
}

void XLogForgetRelation(RelFileNode node) {
  HASH_SEQ_STATUS status;
  xl_invalid_pax_file *hentry;

  if (invalid_pax_file_tab == NULL) return; /* nothing to do */

  hash_seq_init(&status, invalid_pax_file_tab);

  while ((hentry = (xl_invalid_pax_file *)hash_seq_search(&status)) != NULL) {
    if (RelFileNodeEquals(hentry->key.node, node)) {
      if (hash_search(invalid_pax_file_tab, (void *)&hentry->key, HASH_REMOVE,
                      NULL) == NULL) {
        elog(ERROR, "hash table corrupted");
      }
    }
  }
}

void XLogForgetDatabase(Oid dbId) {
  HASH_SEQ_STATUS status;
  xl_invalid_pax_file *hentry;

  if (invalid_pax_file_tab == NULL) return; /* nothing to do */

  hash_seq_init(&status, invalid_pax_file_tab);
  PAX_LOG_IF(pax::pax_enable_debug, "forget database, dbid: %u", dbId);
  while ((hentry = (xl_invalid_pax_file *)hash_seq_search(&status)) != NULL) {
    if (hentry->key.node.dbNode == dbId) {
      if (hash_search(invalid_pax_file_tab, (void *)&hentry->key, HASH_REMOVE,
                      NULL) == NULL) {
        elog(ERROR, "hash table corrupted");
      }
    }
  }
}

void XLogConsistencyCheck() {
  HASH_SEQ_STATUS status;
  xl_invalid_pax_file *hentry;
  bool foundone = false;

  if (invalid_pax_file_tab == NULL) return;

  // do consistency check for pax storage
  hash_seq_init(&status, invalid_pax_file_tab);
  while ((hentry = (xl_invalid_pax_file *)hash_seq_search(&status)) != NULL) {
    elog(WARNING, "pax file is invalid, node: %u/%u/%u, filename: %s",
         hentry->key.node.dbNode, hentry->key.node.spcNode,
         hentry->key.node.relNode, hentry->key.filename);
    foundone = true;
  }

  if (foundone) {
    elog(ignore_invalid_pages ? WARNING : PANIC,
         "PAX WAL contains references to invalid pages");
  }

  hash_destroy(invalid_pax_file_tab);
  invalid_pax_file_tab = NULL;
}

void XLogPaxInsert(RelFileNode node, const char *filename, int64 offset,
                   void *buffer, int32 bufferLen) {
  int file_name_len = strlen(filename);

  if (file_name_len >= MAX_PATH_FILE_NAME_LEN) {
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("filename length is too long: %u and will truncate",
                           file_name_len)));
  }

  xl_pax_insert xlrec;
  xlrec.target.node = node;
  xlrec.target.file_name_len = file_name_len;
  xlrec.target.offset = offset;

  XLogBeginInsert();
  XLogRegisterData((char *)&xlrec, SizeOfPAXInsert);
  XLogRegisterData((char *)filename, xlrec.target.file_name_len);

  if (bufferLen != 0) XLogRegisterData((char *)buffer, bufferLen);

  SIMPLE_FAULT_INJECTOR("XLogPaxInsert");

  XLogRecPtr lsn = XLogInsert(PAX_RMGR_ID, XLOG_PAX_INSERT);
  PAX_LOG_IF(pax::pax_enable_debug,
             "pax xlog insert, node: %u/%u/%u,filename: %s, offset: %ld, "
             "bufferLen: %d, "
             "xlog_ptr: %X/%X",
             node.dbNode, node.spcNode, node.relNode, filename, offset,
             bufferLen, (uint32)(lsn >> 32), (uint32)lsn);

  // FIXME(gongxun): we should wait for mirror here,copy from xlog_ao_insert
  // wait to avoid large replication lag
  wait_to_avoid_large_repl_lag();
}

void XLogPaxCreateDirectory(RelFileNode node) {
  xl_pax_directory xlrec;
  xlrec.node = node;

  XLogBeginInsert();
  XLogRegisterData((char *)&xlrec, SizeOfPAXDirectory);
  SIMPLE_FAULT_INJECTOR("paxc_xlog_pax_create_directory");

  XLogRecPtr lsn = XLogInsert(PAX_RMGR_ID, XLOG_PAX_CREATE_DIRECTORY);
  PAX_LOG_IF(pax::pax_enable_debug,
             "pax xlog create directory, node: %u/%u/%u, xlog_ptr: %X/%X",
             node.dbNode, node.spcNode, node.relNode, (uint32)(lsn >> 32),
             (uint32)lsn);
}

void XLogPaxTruncate(RelFileNode node) {
  xl_pax_directory xlrec;
  xlrec.node = node;

  XLogBeginInsert();
  XLogRegisterData((char *)&xlrec, SizeOfPAXDirectory);

  SIMPLE_FAULT_INJECTOR("paxc_xlog_pax_truncate");

  XLogRecPtr lsn = XLogInsert(PAX_RMGR_ID, XLOG_PAX_TRUNCATE);
  PAX_LOG_IF(pax::pax_enable_debug,
             "pax xlog truncate, node: %u/%u/%u,  xlog_ptr: %X/%X",
             node.dbNode, node.spcNode, node.relNode, (uint32)(lsn >> 32),
             (uint32)lsn);
}

void XLogRedoPaxInsert(XLogReaderState *record) {
  char *relpath;
  char filepath[MAX_PATH_FILE_NAME_LEN];
  char *path;
  int written_len;
  File file;
  int fileFlags;

  char *rec = XLogRecGetData(record);
  xl_pax_insert *xlrec = (xl_pax_insert *)rec;

  // if directory has been marked as invalid, skip
  if (!IsPaxDirectoryValid(xlrec->target.node)) {
    return;
  }

  // in dfs mode, no wal log for pax storage
  relpath = BuildPaxDirectoryPath(xlrec->target.node, InvalidBackendId);

  Assert(xlrec->target.file_name_len < MAX_PATH_FILE_NAME_LEN);

  memcpy(filepath, rec + SizeOfPAXInsert, xlrec->target.file_name_len);
  filepath[xlrec->target.file_name_len] = '\0';

  path = psprintf("%s/%s", relpath, filepath);

  PAX_LOG_IF(pax::pax_enable_debug,
             "pax xlog redo insert, node: %u/%u/%u, offset: %ld, "
             "path: %s",
             xlrec->target.node.dbNode, xlrec->target.node.spcNode,
             xlrec->target.node.relNode, xlrec->target.offset, path);
  char *buffer = (char *)xlrec + SizeOfPAXInsert + xlrec->target.file_name_len;
  int32 bufferLen =
      XLogRecGetDataLen(record) - SizeOfPAXInsert - xlrec->target.file_name_len;

  PAX_LOG_IF(pax::pax_enable_debug, "pax xlog redo insert, bufferLen: %d",
             bufferLen);

  // mark the directory as invalid if it does not exist
  struct stat st;
  int ret = stat(relpath, &st);
  if (ret != 0) {
    if (errno == ENOENT) {
      PAX_LOG_IF(pax::pax_enable_debug,
                 "directory not exists, node: %u/%u/%u, path: %s",
                 xlrec->target.node.dbNode, xlrec->target.node.spcNode,
                 xlrec->target.node.relNode, path);
      LogInvalidPaxDirctory(xlrec->target.node);
    } else {
      ereport(ignore_invalid_pages ? WARNING : PANIC,
              (errcode_for_file_access(),
               errmsg("could not open file %s, dir is not exists", path)));
    }
    return;
  }

  pfree(relpath);

  if (xlrec->target.offset == 0) {
    // why we need to truncate here?
    // If the previous transaction was abnormal, the file name may be reused.
    // If O_TRUNC is not specified, the tail of the file may be garbage data
    // from the last wal synchronization.
    // for example:
    // tx1: write 1024 bytes to file, and crash
    // tx2: write 512 bytes from offset 0 to same file, the last 512 bytes from
    // offset 512 will be garbage data
    fileFlags = O_RDWR | PG_BINARY | O_CREAT | O_TRUNC;
    file = PathNameOpenFile(path, fileFlags);
  } else {
    fileFlags = O_RDWR | PG_BINARY;
    file = PathNameOpenFile(path, fileFlags);
  }

  if (file < 0) {
    // if the file is not exists, mark the file as invalid
    if (errno == ENOENT) {
      PAX_LOG_IF(pax::pax_enable_debug,
                 "file not exists, node: %u/%u/%u, path: %s",
                 xlrec->target.node.dbNode, xlrec->target.node.spcNode,
                 xlrec->target.node.relNode, path);
      LogInvalidPaxFile(xlrec->target.node, path);
    } else {
      ereport(
          ignore_invalid_pages ? WARNING : PANIC,
          (errcode_for_file_access(),
           errmsg("could not open file %s, flags: %d: %m", path, fileFlags)));
    }
    return;
  }

  written_len = FileWrite(file, buffer, bufferLen, xlrec->target.offset,
                          WAIT_EVENT_COPY_FILE_WRITE);

  if (written_len < 0 || written_len != bufferLen) {
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("failed to write %d bytes in file \"%s\": %m",
                           bufferLen, path)));
  }

  pfree(path);

  FileClose(file);
}

void XLogRedoPaxCreateDirectory(XLogReaderState *record) {
  char *dirpath = NULL;
  char *rec = XLogRecGetData(record);
  xl_pax_directory *xlrec = (xl_pax_directory *)rec;

  TablespaceCreateDbspace(xlrec->node.spcNode, xlrec->node.dbNode, true);
  dirpath = paxc::BuildPaxDirectoryPath(xlrec->node, InvalidBackendId);

  PAX_LOG_IF(pax::pax_enable_debug,
             "pax xlog redo create directory, node: %u/%u/%u, dirpath: %s",
             xlrec->node.dbNode, xlrec->node.spcNode, xlrec->node.relNode,
             dirpath);

  // Like mdcreate, we need to create the directory for pax storage.
  // We may be using the target table space for the first time in this
  // database, so create a per-database subdirectory if needed.
  TablespaceCreateDbspace(xlrec->node.spcNode, xlrec->node.dbNode, true);

  int ret = MakePGDirectory(dirpath);
  if (ret != 0) {
    // if directory already exists, skip
    if (errno == EEXIST) {
      PAX_LOG_IF(pax::pax_enable_debug,
                 "directory already exists, node: %u/%u/%u, dirpath: %s",
                 xlrec->node.dbNode, xlrec->node.spcNode, xlrec->node.relNode,
                 dirpath);
    } else {
      ereport(ignore_invalid_pages ? WARNING : PANIC,
              (errcode_for_file_access(),
               errmsg("failed to create directory \"%s\": %m", dirpath)));
    }
  }
  pfree(dirpath);
}

void XLogRedoPaxTruncate(XLogReaderState *record) {
  char *dirpath = NULL;
  char *rec = XLogRecGetData(record);
  xl_pax_directory *xlrec = (xl_pax_directory *)rec;
  dirpath = paxc::BuildPaxDirectoryPath(xlrec->node, InvalidBackendId);

  PAX_LOG_IF(pax::pax_enable_debug,
             "pax xlog redo truncate, node: %u/%u/%u, dirpath: %s",
             xlrec->node.dbNode, xlrec->node.spcNode, xlrec->node.relNode,
             dirpath);

  paxc::DeletePaxDirectoryPath(dirpath, false);
  PAX_LOG_IF(pax::pax_enable_debug, "pax xlog redo truncate, dirpath: %s",
             dirpath);
  pfree(dirpath);
}

}  // namespace paxc

extern "C" {
static void pax_rmgr_startup(void) {}

static void pax_rmgr_cleanup(void) {}

static void pax_rmgr_redo(XLogReaderState *record) {
  /* like ao table, do not replay PAX XLOG records for crash recovery mode.
   * We do not need to replay PAX XLOG records in this case because fsync
   * is performed on file close.
   */
  if (IsCrashRecoveryOnly()) return;

  uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
  switch (info) {
    case XLOG_PAX_INSERT: {
      paxc::XLogRedoPaxInsert(record);
      break;
    }
    case XLOG_PAX_CREATE_DIRECTORY: {
      paxc::XLogRedoPaxCreateDirectory(record);
      break;
    }
    case XLOG_PAX_TRUNCATE: {
      paxc::XLogRedoPaxTruncate(record);
      break;
    }
    default:
      elog(ERROR, "rmgr pax unknown info: %u", info);
  }
}

static ConsistencyCheck_hook_type xlog_check_consistency_hook_pre = NULL;

static XLOGDropDatabase_hook_type xlog_drop_database_hook_pre = NULL;

void PaxConsistencyCheck() {
  if (xlog_check_consistency_hook_pre) {
    xlog_check_consistency_hook_pre();
  }
  // do consistency check for pax storage
  paxc::XLogConsistencyCheck();
}

void PaxXLogDropDatabase(Oid dbid) {
  if (xlog_drop_database_hook_pre) {
    xlog_drop_database_hook_pre(dbid);
  }
  // forget the database for pax storage
  paxc::XLogForgetDatabase(dbid);
}

static RmgrData PaxRmgrData = {.rm_name = "Pax resource manager",
                               .rm_redo = pax_rmgr_redo,
                               .rm_desc = pax_rmgr_desc,
                               .rm_identify = pax_rmgr_identify,
                               .rm_startup = pax_rmgr_startup,
                               .rm_cleanup = pax_rmgr_cleanup,
                               .rm_mask = NULL,
                               .rm_decode = NULL};
}  // extern "C"

void paxc::RegisterPaxRmgr() {
  xlog_check_consistency_hook_pre = xlog_check_consistency_hook;
  xlog_check_consistency_hook = PaxConsistencyCheck;
  xlog_drop_database_hook_pre = XLOGDropDatabase_hook;
  XLOGDropDatabase_hook = PaxXLogDropDatabase;
  RegisterCustomRmgr(PAX_RMGR_ID, &PaxRmgrData);
}
