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
 * paxc_smgr.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/paxc_smgr.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/paxc_smgr.h"

#include "comm/paxc_wrappers.h"
#include "storage/paxc_smgr.h"
#include "storage/wal/paxc_wal.h"

#include <unistd.h>

smgr_get_impl_hook_type prev_smgr_get_impl_hook = NULL;
extern smgr_get_impl_hook_type smgr_get_impl_hook;

namespace paxc {

static void mdunlink_pax(RelFileNodeBackend rnode, ForkNumber forkNumber,
                         bool isRedo) {
  // remove the data directory of pax table
  // FIXME(gongxun): can work well with dfs_tablespace
  if (forkNumber == MAIN_FORKNUM) {
    const char *path =
        paxc::BuildPaxDirectoryPath(rnode.node, rnode.backend);
    paxc::DeletePaxDirectoryPath(path, true);

    if (isRedo) {
      paxc::XLogForgetRelation(rnode.node);
    }
  }

  // unlink the relfilenode file directly, mdunlink will not remove
  // the relfilenode file, only truncate it if isRedo is false.
  auto relpath = relpath(rnode, MAIN_FORKNUM);
  auto ret = unlink(relpath);
  if (ret == -1 && errno != ENOENT) {
    ereport(WARNING, (errcode_for_file_access(),
                      errmsg("PAX: could not remove file \"%s\": %m", relpath)));
  }
  pfree(relpath);
}

static const f_smgr pax_smgr = {
    .smgr_name = "pax",
    .smgr_init = mdinit,
    .smgr_shutdown = NULL,
    .smgr_open = mdopen,
    .smgr_close = mdclose,
    .smgr_create = mdcreate,
    .smgr_exists = mdexists,
    .smgr_unlink = mdunlink_pax,
    .smgr_extend = mdextend,
    .smgr_prefetch = mdprefetch,
    .smgr_read = mdread,
    .smgr_write = mdwrite,
    .smgr_writeback = mdwriteback,
    .smgr_nblocks = mdnblocks,
    .smgr_truncate = mdtruncate,
    .smgr_immedsync = mdimmedsync,
};

void pax_smgr_get_impl_hook(const Relation rel, SMgrImpl *smgr_impl) {
  Assert(rel != NULL);

  if (RelationIsPAX(rel)) {
    *smgr_impl = SMGR_PAX;
  } else {
    if (prev_smgr_get_impl_hook) {
      prev_smgr_get_impl_hook(rel, smgr_impl);
    }
  }
}

void RegisterPaxSmgr() {
  smgr_register(&pax_smgr, SMGR_PAX);

  prev_smgr_get_impl_hook = smgr_get_impl_hook;
  smgr_get_impl_hook = pax_smgr_get_impl_hook;
}

}  // namespace paxc
