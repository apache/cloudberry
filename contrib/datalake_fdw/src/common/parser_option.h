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
 * parser_option.h
 *	  Typed accessors over a DefElem option list.
 *
 * Every layer that reads SERVER, USER MAPPING or table options goes through
 * these accessors rather than walking the list itself, so that lookup and
 * absent-versus-empty are decided in one place.
 *
 * The reference implementation -- the existing implementation of this feature
 * that this work derives from and is meant to replace -- also carries integer
 * and defaulting-boolean accessors (getIntOption, getBoolOption).  They are
 * omitted here rather than shipped unused; add them under those names, as thin
 * wrappers over the accessors below, together with the first option that needs
 * them.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/parser_option.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PARSER_OPTION_H
#define PARSER_OPTION_H

#include "postgres.h"

#include "nodes/pg_list.h"

/* Reference implementation: getStringOption() */
extern char *get_string_option(List *options, const char *option_name);

/* Reference implementation: getBoolOptionEx() */
extern bool get_bool_option_ex(List *options, const char *option_name,
							   bool default_value, bool *isset);

#endif							/* PARSER_OPTION_H */
