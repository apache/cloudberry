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
 * pg_iceberg_guc.c
 *	  Configuration variables for Iceberg table creation.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_guc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "am_iceberg/pg_iceberg_guc.h"
#include "utils/guc.h"

char	   *iceberg_default_catalog;
char	   *iceberg_default_volume;

void
pg_iceberg_define_gucs(void)
{
	/*
	 * Do not install check hooks for these names.  The servers they name are
	 * not necessarily present at assignment time, and assignment happens at
	 * different moments on the coordinator and on the segments.  CREATE TABLE
	 * validates the values against the catalog the executing backend sees.
	 */
	DefineCustomStringVariable("iceberg.default_catalog",
							   "Default catalog server for new Iceberg tables.",
							   NULL,
							   &iceberg_default_catalog,
							   "",
							   PGC_USERSET,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("iceberg.default_volume",
							   "Default volume server for new Iceberg tables.",
							   NULL,
							   &iceberg_default_volume,
							   "",
							   PGC_USERSET,
							   0,
							   NULL,
							   NULL,
							   NULL);
}
