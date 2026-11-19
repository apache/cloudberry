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
 * ansertlist.h
 *    Deparse expressions for post-planning pass-through filter scans.
 *
 *-------------------------------------------------------------------------
 */
#ifndef ANSERTLIST_H
#define ANSERTLIST_H

#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "parser/parsetree.h"

/* INDEX_VAR in a child's output belongs to the child's deparse namespace.
 * Resolve it before copying the expression into a new CustomScan namespace;
 * otherwise stacked filters can make ruleutils resolve a Var to itself.
 * Builders using this helper keep their custom_scan_tlist resolved, so one
 * substitution also handles arbitrarily deep stacks and reordered outputs. */
static inline Node *
anser_filter_tlist_mutator(Node *node, void *context)
{
	CustomScan *child = (CustomScan *) context;

	if (node == NULL)
		return NULL;
	if (IsA(node, Var) && ((Var *) node)->varno == INDEX_VAR)
	{
		TargetEntry *tle = get_tle_by_resno(child->custom_scan_tlist,
										   ((Var *) node)->varattno);
		if (tle == NULL)
			elog(ERROR, "missing child filter output column %d", ((Var *) node)->varattno);
		return (Node *) copyObjectImpl(tle->expr);
	}
	return expression_tree_mutator(node, anser_filter_tlist_mutator, context);
}

static inline List *
AnserFilterScanTlist(Plan *child)
{
	if (IsA(child, CustomScan))
		return (List *) expression_tree_mutator((Node *) child->targetlist,
			anser_filter_tlist_mutator, child);
	return (List *) copyObjectImpl(child->targetlist);
}

#endif /* ANSERTLIST_H */
