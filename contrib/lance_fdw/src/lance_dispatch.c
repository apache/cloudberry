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
 * lance_dispatch.c
 *	  MPP dispatch of Lance fragments.
 *
 * Cloudberry sends one plan to every segment; there is no mechanism for giving
 * each QE a different one.  So the split is not sent, it is recomputed: the QD
 * puts the whole fragment list into the plan, and every QE derives its own
 * share from values the statement already shares - the session id, the command
 * counter and its own segment index.  Two QEs of one statement therefore never
 * pick the same fragment, and between them they pick all of them, without
 * a round trip.
 *
 * The randomising terms are what keeps a short fragment list from always
 * landing on segment 0; the model is gpcontrib/pxf_fdw, which has been running
 * this arithmetic in production.
 *
 * IDENTIFICATION
 *	  src/lance_dispatch.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <stdlib.h>

#include "lance_fdw.h"
#include "lance_dispatch.h"

#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "utils/memutils.h"

#define LANCE_UNIT_KIND_FRAGMENT "fragment"
#define LANCE_UNIT_KIND_NEAREST "nearest"

/* Positions inside one fragment unit. */
#define LANCE_UNIT_KIND		0
#define LANCE_UNIT_URI		1
#define LANCE_UNIT_VERSION	2
#define LANCE_UNIT_IDS		3
#define LANCE_UNIT_SEGMENTS	4
#define LANCE_UNIT_NFIELDS	5

/*
 * Positions inside one nearest unit.  The first three
 * are shared with the fragment unit, so the kind can be read before the
 * length is known to be right for it.
 */
#define LANCE_NEAREST_SEGMENTS	3
#define LANCE_NEAREST_TARGET	4
#define LANCE_NEAREST_VECTOR	5
#define LANCE_NEAREST_NPROBES	6
#define LANCE_NEAREST_REFINE	7
#define LANCE_NEAREST_IDS		8	/* " " for a single search */
#define LANCE_NEAREST_NFIELDS	9

/* One float4 as hex: its 32 bits, eight characters. */
#define LANCE_HEX_PER_ELEMENT	8

char *
lance_dispatch_ids_string(const uint64 *ids, int nids)
{
	StringInfoData buf;
	int			i;

	initStringInfo(&buf);
	for (i = 0; i < nids; i++)
	{
		if (i > 0)
			appendStringInfoChar(&buf, ' ');
		appendStringInfo(&buf, UINT64_FORMAT, ids[i]);
	}

	return buf.data;
}

static List *
lance_dispatch_make_units(const char *uri, uint64 version, int nsegments,
						  const uint64 *ids, int nids)
{
	List	   *unit = NIL;
	char	   *ids_text = lance_dispatch_ids_string(ids, nids);

	/*
	 * A dataset with no fragments would otherwise put an empty String node
	 * into the plan, and whether one of those survives plan serialisation is
	 * not something to bet an empty dataset on.  One space reads back as no
	 * ids, and spaces are escaped by the serialiser like any other.
	 */
	if (ids_text[0] == '\0')
	{
		pfree(ids_text);
		ids_text = pstrdup(" ");
	}

	unit = lappend(unit, makeString(pstrdup(LANCE_UNIT_KIND_FRAGMENT)));
	unit = lappend(unit, makeString(pstrdup(uri)));
	unit = lappend(unit, makeString(psprintf(UINT64_FORMAT, version)));
	unit = lappend(unit, makeString(ids_text));
	unit = lappend(unit, makeString(psprintf("%d", nsegments)));

	return unit;
}

/*
 * Give back a unit list this function put in the plan on an earlier execution.
 * Only this file ever writes that slot, so everything in it is ours to free.
 */
static void
lance_dispatch_free_units(List *units)
{
	ListCell   *lc;

	foreach(lc, units)
	{
		String	   *value = (String *) lfirst(lc);

		if (value == NULL || !IsA(value, String))
			continue;
		if (value->sval != NULL)
			pfree(value->sval);
		pfree(value);
	}

	list_free(units);
}

/*
 * Put `units` into the plan, replacing what a previous execution left there.
 * `units` must have been built in the plan node's own memory context.
 */
static void
lance_dispatch_install(ForeignScan *fsplan, List *units)
{
	if (list_length(fsplan->fdw_private) > LANCE_FDW_PRIVATE_UNITS)
	{
		/*
		 * A cached plan reaches this a second time.  Freeing what the previous
		 * execution left is what keeps a prepared statement in a loop from
		 * growing the plan's memory context one uri and one fragment list at a
		 * time.
		 */
		lance_dispatch_free_units((List *) list_nth(fsplan->fdw_private,
													LANCE_FDW_PRIVATE_UNITS));
		fsplan->fdw_private = list_truncate(fsplan->fdw_private,
											LANCE_FDW_PRIVATE_UNITS);
	}

	fsplan->fdw_private = lappend(fsplan->fdw_private, units);
}

void
lance_dispatch_publish(ForeignScan *fsplan, const char *uri, uint64 version,
					   int nsegments, const uint64 *ids, int nids)
{
	/*
	 * The list has to outlive this executor run, because what reads it is the
	 * plan serialiser on the way to the segments - and, if this plan is a
	 * cached one, the next execution of the same statement.  So build it in
	 * whatever context the plan node itself lives in, and replace any units a
	 * previous execution left behind instead of appending a second set.
	 */
	MemoryContext plancxt = GetMemoryChunkContext(fsplan);
	MemoryContext oldcxt = MemoryContextSwitchTo(plancxt);

	lance_dispatch_install(fsplan,
						   lance_dispatch_make_units(uri, version, nsegments,
													 ids, nids));

	MemoryContextSwitchTo(oldcxt);
}

void
lance_dispatch_publish_nearest(ForeignScan *fsplan, const char *uri,
							   uint64 version, int nsegments, int target,
							   const float4 *vector, int dim, int nprobes,
							   int refine_factor, const uint64 *ids, int nids)
{
	MemoryContext plancxt = GetMemoryChunkContext(fsplan);
	MemoryContext oldcxt = MemoryContextSwitchTo(plancxt);
	List	   *unit = NIL;
	StringInfoData hex;
	int			i;

	char	   *ids_text;

	Assert(dim > 0 && target < nsegments &&
		   (target >= 0 || target == LANCE_SEARCH_DISTRIBUTED));
	Assert(nprobes >= 0 && refine_factor >= 0);

	/* " " for none, for the reason lance_dispatch_make_units gives. */
	ids_text = target == LANCE_SEARCH_DISTRIBUTED
		? lance_dispatch_ids_string(ids, nids) : pstrdup("");
	if (ids_text[0] == '\0')
	{
		pfree(ids_text);
		ids_text = pstrdup(" ");
	}

	initStringInfo(&hex);
	for (i = 0; i < dim; i++)
	{
		uint32		bits;

		memcpy(&bits, &vector[i], sizeof(bits));
		appendStringInfo(&hex, "%08x", bits);
	}

	unit = lappend(unit, makeString(pstrdup(LANCE_UNIT_KIND_NEAREST)));
	unit = lappend(unit, makeString(pstrdup(uri)));
	unit = lappend(unit, makeString(psprintf(UINT64_FORMAT, version)));
	unit = lappend(unit, makeString(psprintf("%d", nsegments)));
	unit = lappend(unit, makeString(psprintf("%d", target)));
	unit = lappend(unit, makeString(hex.data));
	unit = lappend(unit, makeString(psprintf("%d", nprobes)));
	unit = lappend(unit, makeString(psprintf("%d", refine_factor)));
	unit = lappend(unit, makeString(ids_text));

	lance_dispatch_install(fsplan, unit);

	MemoryContextSwitchTo(oldcxt);
}

int
lance_dispatch_search_target(int nsegments)
{
	int64		target;

	Assert(nsegments > 0);
	target = ((int64) gp_session_id % nsegments + nsegments) % nsegments;
	if (target < 0 || target >= nsegments)
		elog(ERROR, "lance_fdw: search segment " INT64_FORMAT " is outside [0, %d)",
			 target, nsegments);
	return (int) target;
}

static uint64
lance_dispatch_parse_uint64(const char *s, const char *what)
{
	char	   *endptr;
	unsigned long long parsed;

	errno = 0;
	parsed = strtoull(s, &endptr, 10);
	if (errno != 0 || endptr == s || *endptr != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("lance_fdw: cannot read the %s in the plan", what),
				 errdetail("Found \"%s\".", s)));

	return (uint64) parsed;
}

/*
 * Split the space separated id list.  An empty string means no fragments,
 * which is what an empty dataset looks like.
 */
static void
lance_dispatch_parse_ids(const char *s, uint64 **ids, int *nids)
{
	const char *p;
	int			count = 0;
	int			i = 0;

	for (p = s; *p != '\0';)
	{
		while (*p == ' ')
			p++;
		if (*p == '\0')
			break;
		count++;
		while (*p != '\0' && *p != ' ')
			p++;
	}

	*nids = count;
	*ids = count > 0 ? (uint64 *) palloc(sizeof(uint64) * count) : NULL;

	for (p = s; *p != '\0';)
	{
		char	   *endptr;
		unsigned long long parsed;

		while (*p == ' ')
			p++;
		if (*p == '\0')
			break;

		errno = 0;
		parsed = strtoull(p, &endptr, 10);
		if (errno != 0 || endptr == p || (*endptr != ' ' && *endptr != '\0'))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("lance_fdw: cannot read the fragment list in the plan"),
					 errdetail("Found \"%s\".", s)));

		(*ids)[i++] = (uint64) parsed;
		p = endptr;
	}

	Assert(i == count);
}

static void lance_dispatch_bad_unit(const char *what, const char *found) pg_attribute_noreturn();

/* A protocol violation: the plan is not one this build wrote. */
static void
lance_dispatch_bad_unit(const char *what, const char *found)
{
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("lance_fdw: the scan unit in the plan has an invalid %s", what),
			 errdetail("Found \"%s\".", found)));
}

/* The String at `pos` of a unit; anything else is not a unit this build wrote. */
static const char *
lance_dispatch_field(List *unit, int pos)
{
	Node	   *node = (Node *) list_nth(unit, pos);

	if (node == NULL || !IsA(node, String))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("lance_fdw: scan unit field %d in the plan is not a string", pos)));
	return strVal(node);
}

static int
lance_dispatch_parse_int(const char *s, const char *what, int min, int max)
{
	char	   *endptr;
	long		parsed;

	errno = 0;
	parsed = strtol(s, &endptr, 10);
	if (errno != 0 || endptr == s || *endptr != '\0' ||
		parsed < min || parsed > max)
		lance_dispatch_bad_unit(what, s);

	return (int) parsed;
}

/*
 * The query vector back from its hex form.  Every element is exactly eight hex
 * digits, so the length alone says how many there are, and anything else in
 * the string is a broken plan rather than something to skip.
 */
static void
lance_dispatch_parse_vector(const char *s, float4 **vector, int *dim)
{
	size_t		len = strlen(s);
	size_t		n;
	size_t		i;

	if (len == 0 || len % LANCE_HEX_PER_ELEMENT != 0 ||
		len / LANCE_HEX_PER_ELEMENT > (size_t) Min(PG_INT32_MAX,
												  MaxAllocSize / sizeof(float4)))
		lance_dispatch_bad_unit("query vector", s);

	n = len / LANCE_HEX_PER_ELEMENT;
	*vector = (float4 *) palloc(sizeof(float4) * n);
	*dim = (int) n;

	for (i = 0; i < n; i++)
	{
		uint32		bits = 0;
		int			j;

		for (j = 0; j < LANCE_HEX_PER_ELEMENT; j++)
		{
			char		c = s[i * LANCE_HEX_PER_ELEMENT + j];
			int			v;

			if (c >= '0' && c <= '9')
				v = c - '0';
			else if (c >= 'a' && c <= 'f')
				v = c - 'a' + 10;
			else
				lance_dispatch_bad_unit("query vector", s);
			bits = (bits << 4) | (uint32) v;
		}
		memcpy(&(*vector)[i], &bits, sizeof(bits));
	}
}

bool
lance_dispatch_read_units(const ForeignScan *fsplan, LanceScanUnits *out)
{
	List	   *unit;
	const char *kind;

	if (list_length(fsplan->fdw_private) <= LANCE_FDW_PRIVATE_UNITS)
		return false;

	unit = (List *) list_nth(fsplan->fdw_private, LANCE_FDW_PRIVATE_UNITS);
	if (list_length(unit) < 3)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("lance_fdw: the plan carries %d scan unit fields",
						list_length(unit))));

	memset(out, 0, sizeof(*out));
	kind = lance_dispatch_field(unit, LANCE_UNIT_KIND);
	out->uri = (char *) lance_dispatch_field(unit, LANCE_UNIT_URI);
	out->version = lance_dispatch_parse_uint64(lance_dispatch_field(unit, LANCE_UNIT_VERSION),
											   "dataset version");

	if (strcmp(kind, LANCE_UNIT_KIND_FRAGMENT) == 0)
	{
		if (list_length(unit) != LANCE_UNIT_NFIELDS)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("lance_fdw: the plan carries %d scan unit fields, expected %d",
							list_length(unit), LANCE_UNIT_NFIELDS)));

		out->kind = LANCE_UNIT_FRAGMENT;
		lance_dispatch_parse_ids(lance_dispatch_field(unit, LANCE_UNIT_IDS),
								 &out->ids, &out->nids);
		out->nsegments = (int) lance_dispatch_parse_uint64(lance_dispatch_field(unit, LANCE_UNIT_SEGMENTS),
														   "segment count");
		return true;
	}

	if (strcmp(kind, LANCE_UNIT_KIND_NEAREST) == 0)
	{
		if (list_length(unit) != LANCE_NEAREST_NFIELDS)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("lance_fdw: the plan carries %d scan unit fields, expected %d",
							list_length(unit), LANCE_NEAREST_NFIELDS)));

		out->kind = LANCE_UNIT_NEAREST;
		out->nsegments = lance_dispatch_parse_int(lance_dispatch_field(unit, LANCE_NEAREST_SEGMENTS),
												  "segment count", 1, PG_INT32_MAX);
		out->target = lance_dispatch_parse_int(lance_dispatch_field(unit, LANCE_NEAREST_TARGET),
											   "search segment",
											   LANCE_SEARCH_DISTRIBUTED,
											   out->nsegments - 1);
		lance_dispatch_parse_vector(lance_dispatch_field(unit, LANCE_NEAREST_VECTOR),
									&out->vector, &out->dim);
		out->nprobes = lance_dispatch_parse_int(lance_dispatch_field(unit, LANCE_NEAREST_NPROBES),
												"nprobes", 0, PG_INT32_MAX);
		out->refine_factor = lance_dispatch_parse_int(lance_dispatch_field(unit, LANCE_NEAREST_REFINE),
													  "refine_factor", 0, PG_INT32_MAX);
		lance_dispatch_parse_ids(lance_dispatch_field(unit, LANCE_NEAREST_IDS),
								 &out->ids, &out->nids);
		/* A single search reads the whole dataset; it carries no fragments. */
		if (out->target != LANCE_SEARCH_DISTRIBUTED && out->nids != 0)
			lance_dispatch_bad_unit("fragment list", lance_dispatch_field(unit, LANCE_NEAREST_IDS));
		return true;
	}

	/*
	 * A unit kind this build does not know can only come from a newer plan,
	 * which is not something to guess at.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("lance_fdw: unknown scan unit kind \"%s\"", kind)));
	return false;				/* keep the compiler quiet */
}

int
lance_dispatch_take_share(uint64 *ids, int nids, int nsegments)
{
	int			mysegment = GpIdentity.segindex;
	int64		shift;
	int			kept = 0;
	int			i;

	/*
	 * The modulus is the width the QD planned for, not the size of the
	 * cluster.  A foreign table or its server may set num_segments, and
	 * Cloudberry then builds the Strewn locus for that many segments
	 * (plancat.c:541 -> pathnode.c:3671), executing the slice on contents
	 * 0..num_segments-1 only.  Dividing the fragments among every content of
	 * the cluster would leave the ones assigned to a segment that never runs
	 * unread, which is silent data loss rather than a slower scan.
	 */
	if (nsegments <= 0 || mysegment < 0)
		return nids;			/* not a QE of a dispatched statement */

	/*
	 * A segment outside the planned width has no share.  It should not be
	 * running this slice at all; reading anything here would duplicate what a
	 * participating segment already read.
	 */
	if (mysegment >= nsegments)
		return 0;

	/*
	 * Both terms are the same on every QE of one statement, which is the whole
	 * reason the shares agree.  They are normalised into [0, nsegments) first:
	 * C's % keeps the sign of its left operand, and a negative shift would
	 * leave some fragments belonging to no segment at all.
	 */
	shift = ((int64) gp_session_id % nsegments + (int64) gp_command_count) % nsegments;
	if (shift < 0)
		shift += nsegments;

	for (i = 0; i < nids; i++)
	{
		if ((i + shift) % nsegments == mysegment)
			ids[kept++] = ids[i];
	}

	return kept;
}

int
lance_dispatch_take_range(uint64 *ids, int nids, int nsegments)
{
	int			mysegment = GpIdentity.segindex;
	int64		lo;
	int64		hi;

	/* The same two guards as take_share, for the same reasons. */
	if (nsegments <= 0 || mysegment < 0)
		return nids;
	if (mysegment >= nsegments)
		return 0;

	/* int64: nids * nsegments may not fit in an int. */
	lo = (int64) nids * mysegment / nsegments;
	hi = (int64) nids * (mysegment + 1) / nsegments;
	if (lo > 0 && hi > lo)
		memmove(ids, ids + lo, sizeof(uint64) * (hi - lo));

	return (int) (hi - lo);
}
