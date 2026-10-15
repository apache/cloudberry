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
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * anserplan.h
 *	  Post-planning transformation that injects Anser runtime bloom-filter
 *	  producer/consumer nodes into a finished plan tree.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/include/anserplan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ANSERPLAN_H
#define ANSERPLAN_H

#include "nodes/execnodes.h"	/* PlanState, for the pushdown helpers */
#include "nodes/plannodes.h"

/*
 * Post-plan pass: recognize the supported join shape in a finished PlannedStmt
 * and inject an Anser bloom-filter producer (on the hash build side) and a
 * consumer (above the probe scan).  Called once from planner(), so it covers
 * both the Postgres planner and ORCA.  A no-op unless the Anser runtime-filter
 * GUCs are on and this is a coordinator SELECT.
 */
extern PGDLLEXPORT void AnserApplyRuntimeFilters(PlannedStmt *stmt);

/*
 * Register the two CustomScan providers (producer, consumer) so their methods
 * resolve by name when a dispatched plan is deserialized.  Must run once per
 * backend (QD and every QE) before any plan execution.
 */
extern PGDLLEXPORT void AnserRegisterRuntimeFilterMethods(void);

/*
 * Node builders (implemented in anserplanexec.c, where the CustomScan method
 * tables live).  Each wraps `child` in a pass-through CustomScan carrying the
 * runtime-filter parameters in custom_private; the caller assigns plan_node_id.
 * `key_attno` is the build (producer) / probe (consumer) join-key attno in the
 * child's output tuple.  `n_producers` is how many processes will publish a
 * part -- the width of the build scan's slice, which under parallel execution
 * is numsegments * parallel_workers rather than the segment count.
 */
/*
 * True for the producer and consumer nodes this module injects.  The injection
 * pass meets them where it expects a scan, because two joins can want to filter
 * the same base relation; coordinator-only, see the implementation.
 */
extern PGDLLEXPORT bool AnserIsRuntimeFilterScan(const Plan *plan);

extern PGDLLEXPORT CustomScan *AnserBuildBloomProducerScan(
										Plan *child,
										AttrNumber key_attno,
										uint32 condition_id,
										const char *condition_key,
										int64 total_elems,
										Size max_payload_bytes,
										int64 planned_bytes,
										int n_producers);
/*
 * `defer_first` tells the consumer to pass its first tuple through unfiltered.
 * ExecHashJoin pulls one outer tuple before building its hash table, and a
 * consumer that blocks there waits for an inner side the join has not started
 * -- in a co-located join, for its own process.  Set it whenever the prefetch
 * cannot be ruled out; the cost is that such a consumer cannot push its filter
 * into the scan and probes it itself instead.
 */
extern PGDLLEXPORT CustomScan *AnserBuildBloomConsumerScan(
										Plan *child,
										AttrNumber key_attno,
										uint32 condition_id,
										const char *condition_key,
										int64 total_elems,
										Size max_payload_bytes,
										int64 planned_bytes,
										int n_producers,
										bool defer_first);

/*
 * Bloom sizing for one join, from its estimated build cardinality.  False means
 * no filter is worth injecting -- see the density rule in anserfilter.h.
 *
 * Exposed rather than static because it is the cheapest of Anser's give-up
 * decisions and therefore the one most worth testing directly at its boundary
 * (anser_test.c); nothing but the injection pass and the tests should call it.
 */
extern PGDLLEXPORT bool AnserRuntimeFilterSize(double est_rows,
											   int64 *total_elems,
											   int64 *max_payload,
											   int64 *planned_bytes);

/*
 * Follow a key column of `top` down to the base scan that originates it,
 * through the nodes that pass a column through unchanged (Hash, Motion), the
 * joins that carry one side's columns up (HashJoin), and our own nodes.  True
 * on success, with *scan_out the scan, *attno_out the key's attno in it,
 * *parent_out the node holding it (ours keep it in custom_plans, everything
 * else in the outer link) and *slice_out the slice the scan runs in.
 *
 * False means no single base scan holds the whole key column -- an Append or a
 * SetOp, say, where the column comes from several relations at once and a
 * filter built on any one of them would reject rows that can join.  A false
 * return is therefore a correctness gate, not a missed optimization: callers
 * must decline rather than guess.
 *
 * Only meaningful after set_plan_references, which is also what makes the
 * descent necessary: before it the Vars still name their base relations.
 */
extern PGDLLEXPORT bool AnserResolveKeyScan(Plan *top,
											AttrNumber key_attno,
											int slice_index,
											Plan **parent_out,
											Plan **scan_out,
											AttrNumber *attno_out,
											int *slice_out);

/*
 * How many processes execute `slice_index` of `stmt` -- the number of parts a
 * channel fed from that slice must wait for.  Zero means the slice could not be
 * identified, and the caller must decline rather than assume: a channel that
 * expects too few parts completes early and hands out a filter missing some
 * segment's keys, which drops rows that can join.
 *
 * Not the segment count.  A parallel slice runs numsegments * parallel_workers
 * full QEs, each publishing for itself, and the two optimizers choose different
 * widths for the same query -- see the implementation.
 */
extern PGDLLEXPORT int AnserSliceProducers(PlannedStmt *stmt, int slice_index);

/*
 * Runtime-filter pushdown, split so the decision can be tested on its own.
 *
 * AnserPushdownAccepts answers "would this node take a scan key for the column
 * it emits at output position `attno`?", and on yes reports the attribute
 * number the key must carry -- which is the table's attno, not `attno`, because
 * the scan evaluates the key against an unprojected tuple.
 *
 * AnserPushdownTarget walks down from `top` following that column and returns
 * the deepest accepting node, so a stack of consumers over one relation all
 * push into the same scan instead of probing tuple by tuple.
 *
 * Both return false/NULL rather than failing when the shape is not supported;
 * the caller's fallback is to probe the filter itself, which is always correct.
 */
extern PGDLLEXPORT bool AnserPushdownAccepts(PlanState *ps, AttrNumber attno,
											 AttrNumber *sk_attno_out);
extern PGDLLEXPORT PlanState *AnserPushdownTarget(PlanState *top,
												  AttrNumber attno,
												  AttrNumber *sk_attno_out);

#endif							/* ANSERPLAN_H */
