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
 * mdb_locale.h
 *	  Generic headers for custom MDB-locales patch.
 *
 * IDENTIFICATION
 *		  src/include/common/mdb_locale.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_MDB_LOCALE_H
#define PG_MDB_LOCALE_H

#ifdef USE_MDBLOCALES
#include <mdblocales.h>
#define SETLOCALE(category, locale) mdb_setlocale(category, locale)
#define NEWLOCALE(category, locale, base) mdb_newlocale(category, locale, base)
#else
#define SETLOCALE(category, locale) setlocale(category, locale)
#define NEWLOCALE(category, locale, base) newlocale(category, locale, base)
#endif

#endif							/* PG_MDB_LOCALE_H */
