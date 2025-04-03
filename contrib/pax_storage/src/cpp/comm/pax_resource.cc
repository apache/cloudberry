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
 * pax_resource.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/pax_resource.cc
 *
 *-------------------------------------------------------------------------
 */

#include "comm/pax_resource.h"

#include <mutex>

namespace pax {
namespace common {

struct PaxResourceContext {
  struct dlist_node node;
  ResourceOwner owner;
  ResourceReleaseFunc release;
  Datum arg;
};

struct PaxResourceList {
  PaxResourceList() {
    dlist_init(&head);
  }
  struct dlist_head head;
};

static PaxResourceList resource_list;
static std::mutex resource_list_lock;

void InitResourceCallback() {
  Assert(dlist_is_empty(&resource_list.head));
  RegisterResourceReleaseCallback(paxc::ReleaseResourceCallback, NULL);
}

bool RememberResourceCallback(ResourceReleaseFunc release_func, Datum arg) {
  struct PaxResourceContext *ctx;

  ctx = static_cast<PaxResourceContext *>(malloc(sizeof(*ctx)));
  if (!ctx) return false;

  ctx->owner = CurrentResourceOwner;
  ctx->release = release_func;
  ctx->arg = arg;

  {
    std::lock_guard<std::mutex> lock(resource_list_lock);
    dlist_push_tail(&resource_list.head, &ctx->node);
  }
  return true;
}

// Like dlist_foreach_modify, but remove the unsupported builtin
// function(__builtin_types_compatible_p) in c++.
#define pax_dlist_foreach_modify(iter, lhead)                         \
  for ((iter).end = &(lhead)->head,                                   \
     (iter).cur = (iter).end->next ? (iter).end->next : (iter).end,   \
     (iter).next = (iter).cur->next;                                  \
     (iter).cur != (iter).end;                                        \
     (iter).cur = (iter).next, (iter).next = (iter).cur->next)

bool ForgetResourceCallback(ResourceReleaseFunc release_func, Datum arg) {
  dlist_mutable_iter iter;
  PaxResourceContext *ctx = nullptr;
  std::lock_guard<std::mutex> lock(resource_list_lock);

  pax_dlist_foreach_modify(iter, &resource_list.head) {
    ctx = reinterpret_cast<PaxResourceContext *>(iter.cur);
    if (ctx->release == release_func && ctx->arg == arg) {
      dlist_delete(&ctx->node);
      free(ctx);
      return true;
    }
  }
  return false;
}

}
}

using namespace pax::common;
namespace paxc {
void ReleaseResourceCallback(ResourceReleasePhase phase, bool is_commit,
                             bool /* is_top_level */,
                             void * /*arg*/) {
  if (phase != RESOURCE_RELEASE_AFTER_LOCKS || proc_exit_inprogress)
    return;

  dlist_mutable_iter iter;
  std::lock_guard<std::mutex> lock(pax::common::resource_list_lock);

  pax_dlist_foreach_modify(iter, &pax::common::resource_list.head) {
    auto ctx = reinterpret_cast<PaxResourceContext *>(iter.cur);
    Assert(ctx);
    if (ctx->owner != CurrentResourceOwner) continue;

    if (is_commit) elog(WARNING, "pax resource leaks: %p", ctx->release);

    dlist_delete(&ctx->node);
    ctx->release(ctx->arg);
    free(ctx);
  }

  // after TopTransaction resource owner, our resource list shouldo
  // be empty.
  // NOTE: is_top_level may be true for Sub-Transaction's resource
  // owner, so our resource list could be not empty.
  AssertImply(CurrentResourceOwner == TopTransactionResourceOwner,
              dlist_is_empty(&pax::common::resource_list.head));
}

}
