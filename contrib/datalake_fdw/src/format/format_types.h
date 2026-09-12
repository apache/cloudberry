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
 * format_types.h
 *	  Which PostgreSQL column types a lake table can hold.
 *
 * The answer belongs to the format layer, which is what has to store the
 * values; but the place to give it is CREATE TABLE, before a table exists that
 * can never be written to.  This is the one function both ask, so that they
 * cannot disagree.  It is plain C so that the access method can call it
 * without seeing Arrow.
 *
 * postgres.h must be included before this header.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/format/format_types.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DL_FORMAT_TYPES_H
#define DL_FORMAT_TYPES_H

#ifdef __cplusplus
extern "C"
{
#endif

/*
 * NULL when a column of this type, with this modifier, can be stored in a data
 * file; otherwise a static sentence saying why not, written to follow the name
 * of the type -- "type X, which <reason>".  The type is not named here because
 * naming it means a catalog lookup, and one caller is on the far side of the
 * C++ boundary where nothing may allocate; the callers know the type.
 *
 * A length limit is refused, not stored: varchar(n) and char(n) have nowhere
 * to record n in a lake table, whose only string type is an unbounded UTF-8
 * string, and a limit that lives in the catalog but not in the data would make
 * a file written by anything else able to violate the declared type.  char(n)
 * is refused for its padding as well, which no other reader would strip.
 */
extern const char *dl_format_type_refusal(Oid typid, int32 typmod);

#ifdef __cplusplus
}
#endif

#endif							/* DL_FORMAT_TYPES_H */
