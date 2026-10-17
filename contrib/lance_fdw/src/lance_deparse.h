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
 * lance_deparse.h
 *	  Turning a PostgreSQL qual into a Lance SQL filter, or refusing to.
 *
 * Two passes over the same expression tree, both inside one call from
 * GetForeignPlan: the first decides whether every node is on the
 * whitelist, the second renders.  Either pass may say no, and "no" is an
 * ordinary answer - the clause simply stays with the ForeignScan.  Splitting
 * the two across planner stages would mean the render could no longer refuse,
 * because by then the clause has already been taken out of scan_clauses.
 *
 * IDENTIFICATION
 *	  src/lance_deparse.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LANCE_DEPARSE_H
#define LANCE_DEPARSE_H

#include "lance_fdw.h"

#include "nodes/pathnodes.h"
#include "nodes/primnodes.h"

/*
 * What one successful deparse produced.
 *
 * `attnums` is every column the filter refers to.  They travel to the QE
 * separately from the projection, because the whole point of pushing a
 * qualifier down is that its columns no longer have to be read - but they
 * still have to be checked against the dataset schema.
 */
typedef struct LanceDeparsed
{
	char	   *sql;			/* the rendered filter */
	List	   *attnums;		/* List of Integer: columns it refers to */
} LanceDeparsed;

/*
 * Try to render one qual.  Returns true and fills *out on success; returns
 * false, having written nothing, when the clause is not on the whitelist or
 * cannot be rendered exactly.  Never raises.
 */
extern bool lance_deparse_qual(Expr *expr, Oid foreigntableid,
							   Index varno, LanceDeparsed *out);

/*
 * Does this qual go to Lance?  lance_deparse_qual() with the
 * lance_fdw.enable_filter_pushdown escape hatch applied on top.  This is the
 * one judgement GetForeignPlan and the vector Top-K matcher share: the matcher
 * may only push a LIMIT down when every qual goes to Lance, and asking a second,
 * slightly different question would let the two disagree about a clause that
 * then gets evaluated after Lance has already cut the rows to k.
 */
extern bool lance_deparse_pushdown(Expr *expr, Oid foreigntableid,
								   Index varno, LanceDeparsed *out);

/*
 * Quote a Lance column name for the filter dialect.  Returns NULL when the
 * name cannot be expressed - which today means it contains a backtick, the
 * one character the dialect gives no way to escape.
 */
extern char *lance_deparse_quote_ident(const char *name);

#endif							/* LANCE_DEPARSE_H */
