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
 * lance_vector.h
 *	  Vector distance operators on real[], and recognising the
 *	  ORDER BY distance LIMIT k queries that can go to Lance's nearest-neighbour
 *	  search.
 *
 * IDENTIFICATION
 *	  src/lance_vector.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LANCE_VECTOR_H
#define LANCE_VECTOR_H

#include "lance_fdw.h"

#include "nodes/pathnodes.h"
#include "nodes/primnodes.h"

/*
 * The three distances, numbered as lance-c numbers its metrics so that the
 * value travels to the QE unchanged.  Hamming has no operator here.
 */
typedef enum LanceVectorMetric
{
	LANCE_VECTOR_L2 = LANCE_METRIC_L2,			/* <-> */
	LANCE_VECTOR_COSINE = LANCE_METRIC_COSINE,	/* <=> */
	LANCE_VECTOR_DOT = LANCE_METRIC_DOT			/* <#> */
} LanceVectorMetric;

#define LANCE_VECTOR_NMETRICS 3

/*
 * What the planner found: the column ordered on, the distance, k, and the
 * query vector expression - a Const or an external Param, evaluated by the QD
 * at execution time.
 */
typedef struct LanceTopK
{
	AttrNumber	attnum;
	char	   *column;			/* Lance column name */
	LanceVectorMetric metric;
	int64		k;
	Expr	   *query;
} LanceTopK;

/*
 * Decide whether this scan may become a vector Top-K search, and fill *out if
 * it may.  Every condition is checked here; the first one that
 * fails is the reason, logged at DEBUG1 - but only for a query that orders by
 * one of the three distance operators, so that every other plan stays quiet.
 * Never raises, and does no I/O.
 */
extern bool lance_vector_match(PlannerInfo *root, RelOptInfo *baserel,
							   Oid foreigntableid, LanceTopK *out);

extern const char *lance_vector_metric_name(LanceVectorMetric metric);
extern const char *lance_vector_metric_operator(LanceVectorMetric metric);

#endif							/* LANCE_VECTOR_H */
