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
 * anserdetach_test.c
 *    Connection-event cleanup of Anser deliveries and queued chunks.
 *
 * Runs the production callback without a cluster. PostgreSQL allocation,
 * wait and error-reporting functions are deliberately unavailable: adding a
 * dependency on them to cleanup makes the test fail to link.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/test/anserdetach_test.c
 *
 *-------------------------------------------------------------------------
 */
/* Test the production teardown with allocation, waiting and error-reporting
 * entry points deliberately unavailable. Unused transport code is discarded
 * by the linker; any such call reachable from teardown breaks this test. */
#include "postgres.h"

static bool fail_name;
static bool fail_events;
static int allocation_attempts;
static char *fixture_strdup(const char *text);
static void *fixture_realloc(void *memory, size_t bytes);
#define strdup fixture_strdup
#define realloc fixture_realloc
#include "../src/anserdispatch.c"
#undef strdup
#undef realloc

static char *fixture_strdup(const char *text)
{
	++allocation_attempts;
	return fail_name ? NULL : strdup(text);
}
static void *fixture_realloc(void *memory, size_t bytes)
{
	++allocation_attempts;
	return fail_events ? NULL : realloc(memory, bytes);
}
#include <stdlib.h>

#define CHECK(c) do { if (!(c)) abort(); } while (0)

void pfree(void *p) { free(p); }
void list_free(List *list)
{
	if (list == NIL) return;
	free(list->elements);
	free(list);
}
void ExceptionalCondition(const char *a, const char *b, int c) { abort(); }

static List *
fixture_list(void *a, void *b)
{
	List *list = calloc(1, sizeof(List));
	CHECK(list);
	list->type = T_List;
	list->length = list->max_length = b == NULL ? 1 : 2;
	list->elements = calloc(list->length, sizeof(ListCell));
	CHECK(list->elements);
	list->elements[0].ptr_value = a;
	if (b) list->elements[1].ptr_value = b;
	return list;
}

/* Mirror libpq's documented callback-before-free ordering. */
static void
finish_fixture_conn(PGconn *conn)
{
	int i;

	for (i = 0; i < conn->nEvents; ++i)
	{
		PGEventConnDestroy event = {conn};

		CHECK(conn->events[i].proc(PGEVT_CONNDESTROY, &event,
								  conn->events[i].passThrough));
		free(conn->events[i].name);
	}
	free(conn->events);
	free(conn);
}

static int other_event(PGEventId event, void *info, void *pass_through)
{
	return 1;
}

int main(void)
{
	PGconn *own = calloc(1, sizeof(*own));
	PGconn *other = calloc(1, sizeof(*other));
	AnserDispChannel channel = {0};
	char payload[] = "completed payload";
	AnserDispDelivery *a = calloc(1, sizeof(*a));
	AnserDispDelivery *b = calloc(1, sizeof(*b));
	CHECK(own && other && a && b);
	CHECK(!anser_disp_track_conn(NULL));
	fail_name = true;
	CHECK(!anser_disp_track_conn(own) && own->nEvents == 0);
	fail_name = false;
	fail_events = true;
	CHECK(!anser_disp_track_conn(own) && own->nEvents == 0);
	fail_events = false;
	/* Existing libpq users must keep their event entries and instance data. */
	CHECK(PQregisterEventProc(own, other_event, "other user", NULL));
	CHECK(PQsetInstanceData(own, other_event, &channel));
	CHECK(anser_disp_track_conn(own) && anser_disp_track_conn(other));
	CHECK(anser_disp_track_conn(own) && own->nEvents == 2);
	CHECK(PQinstanceData(own, other_event) == &channel);
	CHECK(!PQregisterEventProc(own, anser_disp_conn_event, "duplicate", NULL));
	own->outCount = other->outCount = 12;
	own->status = other->status = CONNECTION_OK;
	AnserDispPendingPush = fixture_list(own, other);
	a->channel = b->channel = &channel;
	a->conn = own; b->conn = other;
	channel.deliveries = fixture_list(a, b);
	channel.complete = true;
	channel.payload = payload;
	dlist_push_head(&AnserDispDeliveries, &a->active_node);
	dlist_push_head(&AnserDispDeliveries, &b->active_node);

	/* Destroy one connection: its pointers disappear before the memory does.
	 * Other dispatches and the merged payload remain available. */
	{
		int attempts = allocation_attempts;
		finish_fixture_conn(own);
		CHECK(allocation_attempts == attempts);
	}
	CHECK(list_length(AnserDispPendingPush) == 1);
	CHECK(linitial(AnserDispPendingPush) == other);
	CHECK(list_length(channel.deliveries) == 1 && linitial(channel.deliveries) == b);
	CHECK(dlist_head_element(AnserDispDelivery, active_node, &AnserDispDeliveries) == b);
	CHECK(other->status == CONNECTION_OK);

	/* A reset starts another stream on the same PGconn. Forget old state,
	 * but keep registration so later subscriptions remain protected. */
	{
		PGEventConnReset reset = {other};
		CHECK(anser_disp_conn_event(PGEVT_CONNRESET, &reset, NULL));
		CHECK(anser_disp_conn_event(PGEVT_CONNRESET, &reset, NULL));
	}
	CHECK(AnserDispPendingPush == NIL && channel.deliveries == NIL);
	CHECK(dlist_is_empty(&AnserDispDeliveries));
	CHECK(channel.complete && channel.payload == payload);
	CHECK(anser_disp_track_conn(other) && other->nEvents == 1);

	/* Query-end cleanup must not dereference the connection already freed. */
	AnserDispatchFlushPending(10000);
	AnserDispPendingPush = fixture_list(other, NULL);
	AnserDispatchFlushPending(10000);
	CHECK(other->status == CONNECTION_BAD && AnserDispPendingPush == NIL);
	finish_fixture_conn(other);
	CHECK(dlist_is_empty(&AnserDispDeliveries));
	return 0;
}
