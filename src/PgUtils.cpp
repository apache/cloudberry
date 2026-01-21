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
 * PgUtils.cpp
 *
 * IDENTIFICATION
 *	  gpcontrib/yagp_hooks_collector/src/PgUtils.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "PgUtils.h"
#include "Config.h"
#include "memory/gpdbwrappers.h"

extern "C" {
#include "commands/resgroupcmds.h"
#include "cdb/cdbvars.h"
}

std::string get_user_name() {
  // username is allocated on stack, we don't need to pfree it.
  const char *username =
      ya_gpdb::get_config_option("session_authorization", false, false);
  return username ? std::string(username) : "";
}

std::string get_db_name() {
  char *dbname = ya_gpdb::get_database_name(MyDatabaseId);
  if (dbname) {
    std::string result(dbname);
    ya_gpdb::pfree(dbname);
    return result;
  }
  return "";
}

std::string get_rg_name() {
  auto groupId = ya_gpdb::get_rg_id_by_session_id(MySessionState->sessionId);
  if (!OidIsValid(groupId))
    return "";

  char *rgname = ya_gpdb::get_rg_name_for_id(groupId);
  if (rgname == nullptr)
    return "";

  std::string result(rgname);
  ya_gpdb::pfree(rgname);
  return result;
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
  if (query_desc->yagp_query_key == NULL) {
    return nesting_level == 0;
  }
  return query_desc->yagp_query_key->nesting_level == 0;
}
