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
 * dl_option_util.h
 *	  Option policy shared by the catalog and volume option validators.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/dl_option_util.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DL_OPTION_UTIL_H
#define DL_OPTION_UTIL_H

#include "postgres.h"

/*
 * The option keys that identify or authenticate a principal.
 *
 * They are defined here, below the option modules that name them, because
 * dl_is_credential_option() and those modules have to agree: a key the modules
 * accept but this list does not know is a credential the server would store in
 * the clear.  One definition each is what makes disagreement impossible; the
 * per-module macros below are spelled the way the reference implementation
 * spells them and resolve to these.
 */
#define DL_OPTION_KEY_USERNAME "username"
#define DL_OPTION_KEY_KRB_CLIENT_KEYTAB "krb_client_keytab"
#define DL_OPTION_KEY_CLIENT_ID "client_id"
#define DL_OPTION_KEY_CLIENT_SECRET "client_secret"
#define DL_OPTION_KEY_ACCESS_KEY_ID "access_key_id"
#define DL_OPTION_KEY_SECRET_ACCESS_KEY "secret_access_key"
#define DL_OPTION_KEY_SESSION_TOKEN "session_token"

/*
 * True for an option that identifies or authenticates a principal.  Such an
 * option belongs to a user mapping, never to a server, so that one server can
 * be shared by roles with different credentials and so that the value is not
 * readable through pg_foreign_server by every role holding USAGE.
 *
 * Both option validators consult this, which is why it lives here rather than
 * being spelled out twice.
 */
extern bool dl_is_credential_option(const char *name);

#endif							/* DL_OPTION_UTIL_H */
