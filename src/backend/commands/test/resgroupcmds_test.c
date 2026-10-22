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
 * resgroupcmds_test.c
 *
 * IDENTIFICATION
 *	  src/backend/commands/test/resgroupcmds_test.c
 *
 *-------------------------------------------------------------------------
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "../resgroupcmds.c"

/*
 * Helper: simulate running on the coordinator (dispatcher).
 */
static void
set_role_coordinator(void)
{
	GpIdentity.segindex = MASTER_CONTENT_ID;
}

/*
 * Helper: simulate running on a segment.
 */
static void
set_role_segment(void)
{
	GpIdentity.segindex = 0;
}

/*
 * NULL input must raise an ERROR.
 */
static void
test__getCpuSetByRole_null_input(void **state)
{
	PG_TRY();
	{
		getCpuSetByRole(NULL);
		fail_msg("expected ereport(ERROR) for NULL cpuset");
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
}

/*
 * cpuset without a separator: same value must be returned
 * regardless of the role.
 */
static void
test__getCpuSetByRole_no_separator(void **state)
{
	const char *input = "0-7";
	char *result;

	set_role_coordinator();
	result = getCpuSetByRole(input);
	assert_string_equal(result, "0-7");

	set_role_segment();
	result = getCpuSetByRole(input);
	assert_string_equal(result, "0-7");
}

/*
 * cpuset with a separator, called as coordinator:
 * must return the part BEFORE the ';'.
 */
static void
test__getCpuSetByRole_with_separator_as_coordinator(void **state)
{
	const char *input = "0-7;0-15";
	char *result;

	set_role_coordinator();
	result = getCpuSetByRole(input);
	assert_string_equal(result, "0-7");
}

/*
 * cpuset with a separator, called as segment:
 * must return the part AFTER the ';'.
 */
static void
test__getCpuSetByRole_with_separator_as_segment(void **state)
{
	const char *input = "0-7;0-15";
	char *result;

	set_role_segment();
	result = getCpuSetByRole(input);
	assert_string_equal(result, "0-15");
}

int
main(int argc, char *argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test__getCpuSetByRole_null_input),
		unit_test(test__getCpuSetByRole_no_separator),
		unit_test(test__getCpuSetByRole_with_separator_as_coordinator),
		unit_test(test__getCpuSetByRole_with_separator_as_segment)
	};

	MemoryContextInit();

	return run_tests(tests);
}
