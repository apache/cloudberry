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
 * parser_option.c
 *	  Typed accessors over a DefElem option list.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/parser_option.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/defrem.h"
#include "common/parser_option.h"
#include "utils/builtins.h"

/*
 * Return the value of the named option, or NULL when it is absent.
 *
 * The comparison is case-sensitive, matching how the server stores and
 * de-duplicates option names.  Matching case-insensitively here would let
 * "type" and a quoted "TYPE" both be stored -- the generic duplicate check
 * would not see them as the same option -- and then silently return whichever
 * came first, which for a credential is the wrong one to pick at random.
 */
char *
get_string_option(List *options, const char *option_name)
{
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, option_name) == 0)
			return defGetString(def);
	}

	return NULL;
}

/*
 * Boolean accessor that also reports whether the option was written at all.
 *
 * Callers that forward options to the metadata engine need that distinction:
 * an unset boolean may fall back to site configuration, while an explicit
 * false has to override it.
 *
 * Unlike the reference implementation, an unparsable value raises an error
 * instead of silently yielding the default -- a typo in a boolean server
 * option should not read as "you asked for the default".  Stored values are
 * already validated by the option validators, so this only fires on input
 * paths that have not been through them.
 */
bool
get_bool_option_ex(List *options, const char *option_name,
				   bool default_value, bool *isset)
{
	char	   *value = get_string_option(options, option_name);
	bool		parsed_value;

	Assert(isset != NULL);
	*isset = false;

	if (value == NULL)
		return default_value;

	if (!parse_bool(value, &parsed_value))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid boolean value \"%s\" for option \"%s\"",
						value, option_name)));

	*isset = true;
	return parsed_value;
}
