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
 * lance_fdw.h
 *	  Definitions shared by every lance_fdw module.
 *
 * Include this instead of <lance/lance.h> or <nanoarrow/nanoarrow.h>: both
 * declare the Arrow C data interface structs, but only nanoarrow.h defines the
 * ARROW_FLAG_* macros and it guards that whole block on
 * ARROW_FLAG_DICTIONARY_ORDERED.  Pulling in lance.h first would therefore
 * leave the flags undefined and make nanoarrow.h declare ArrowArrayStream a
 * second time, so the order below is not cosmetic.
 *
 * Copyright (c) 2026, Apache Cloudberry (incubating) contributors
 *
 * IDENTIFICATION
 *	  src/lance_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LANCE_FDW_H
#define LANCE_FDW_H

#include "nanoarrow/nanoarrow.h"
#include "lance/lance.h"

/*
 * Row estimate used when the table carries no rows_hint option.  The planner
 * may not open the dataset (no I/O while planning), and lance-c
 * has no cheap row count anyway, so this is a constant.
 */
#define LANCE_FDW_DEFAULT_ROWS 100000.0

#endif							/* LANCE_FDW_H */
