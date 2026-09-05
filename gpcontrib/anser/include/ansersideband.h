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
 * ansersideband.h
 *	  Anser transport over the existing QD <-> QE dispatch connection.
 *
 * Instead of opening a second libpq connection back to the coordinator, a
 * segment executor reuses the connection the dispatcher already holds open:
 * QE -> QD travels as a NOTIFY (the model nextval() uses, see
 * cdb_sequence_nextval_qe in commands/sequence.c), and QD -> QE as a
 * GP_SIDEBAND_MESSAGE the waiting QE reads off its own socket.
 *
 * The two directions are not symmetric, and the wire formats differ for a
 * reason.  A NOTIFY payload is delivered by pq_sendstring(), so it must be a
 * NUL-free C string -- hence the text header and base64 body.  The QD -> QE
 * push is written with pqPutnchar(), which performs no conversion, so the
 * merged filter travels as raw binary.  That matters: the merged payload is
 * sent once per consumer, while each part is sent once.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/include/ansersideband.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ANSERSIDEBAND_H
#define ANSERSIDEBAND_H

#include "anser.h"

struct CdbDispatchResult;		/* #include "cdb/cdbdispatchresult.h" */
struct pgNotify;				/* #include "libpq-fe.h" */

/*
 * NOTIFY channel for QE -> QD Anser traffic.  The dispatcher hands notifies it
 * does not recognize to cdbdisp_notify_hook, where we match on this name.
 */
#define ANSER_NOTIFY_CHANNEL	"anser_rf"

/* First token of every QE -> QD payload; bump when the format changes. */
#define ANSER_WIRE_TAG			"anser1"

/* Message kinds (QE -> QD). */
#define ANSER_WIRE_KIND_PART		'P'		/* a producer's serialized part */
#define ANSER_WIRE_KIND_SUBSCRIBE	'S'		/* a consumer registering interest */

/* Flag bits, shared by both directions. */
#define ANSER_WIRE_F_CANCELLED	0x0001

/*
 * QE side (ansersideband.c).
 *
 * AnserSidebandPublish is fire-and-forget: unlike the libpq transport it does
 * not wait for the coordinator to acknowledge the part.
 *
 * AnserSidebandConsumeWait blocks on this backend's own dispatch socket until
 * the merged payload arrives, the channel is cancelled, or timeout_ms elapses.
 * On success *payload is palloc'd in the caller's context.  A false return
 * always means "run unfiltered", never an error.
 */
extern bool AnserSidebandPublish(const AnserChannelKey *channel_key,
								 uint32 part_index, uint32 total_parts,
								 const void *payload, Size payload_len,
								 bool cancelled);
extern bool AnserSidebandConsumeWait(const AnserChannelKey *channel_key,
									 void **payload, Size *payload_len,
									 bool *cancelled, long timeout_ms);

/*
 * QD side (anserdispatch.c).
 *
 * AnserDispatchNotifyHandler is installed as cdbdisp_notify_hook; it folds
 * arriving parts and pushes the merged payload to subscribers.  The Local
 * variants serve producers and consumers running on the coordinator itself,
 * which have no dispatch connection to themselves and so operate on the same
 * per-query channel table directly.
 */
extern bool AnserDispatchNotifyHandler(struct CdbDispatchResult *dispatchResult,
									   struct pgNotify *notify);
extern bool AnserDispatchLocalPublish(const AnserChannelKey *channel_key,
									  uint32 part_index, uint32 total_parts,
									  const void *payload, Size payload_len,
									  bool cancelled);
extern bool AnserDispatchLocalConsume(const AnserChannelKey *channel_key,
									  void **payload, Size *payload_len,
									  bool *cancelled);

/*
 * Drop per-query state.  AnserSidebandResetAll does both sides and is what
 * the executor/transaction-end callbacks call; the halves are exposed because
 * each lives with the state it owns.
 */
extern void AnserDispatchReset(void);
extern void AnserSidebandResetInbox(void);
extern void AnserSidebandResetAll(void);

#endif							/* ANSERSIDEBAND_H */
