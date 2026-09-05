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
 * anser.h
 *	  Shared definitions for the Anser adaptive information sharing
 *	  subsystem.
 *
 * Anser lets producers on the segments publish a small piece of information
 * about a running query -- today a bloom filter over a join-build key -- have
 * the coordinator combine the per-segment parts, and hand the result back to
 * consumers on the segments, which use it to prune work.
 *
 * Everything travels over the dispatch connection the coordinator already
 * holds open to each segment (see ansersideband.h), so there is no shared
 * memory, no background worker and no second connection to authenticate: a
 * channel exists only in the coordinator backend running the query, for as
 * long as that query runs.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/include/anser.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ANSER_H
#define ANSER_H

#include "postgres.h"

#define ANSER_CONDITION_KEY_SIZE	64

/*
 * Identifies one channel: a single condition within one running command.
 *
 * condition_key is an opaque symbol naming the condition -- the
 * optimizer-generated equivalence-class symbol described in the Anser paper.
 * Channels are only ever compared for equality, never ordered.
 */
typedef struct AnserChannelKey
{
	int			gp_session_id;
	int			gp_command_count;
	uint32		condition_id;
	char		condition_key[ANSER_CONDITION_KEY_SIZE];
} AnserChannelKey;

/* GUCs (defined in anserinit.c). */
extern bool gp_anser_enable;
extern bool gp_anser_runtime_filter;
extern int	gp_anser_max_info_size;
extern int	gp_anser_timeout_ms;

#endif							/* ANSER_H */
