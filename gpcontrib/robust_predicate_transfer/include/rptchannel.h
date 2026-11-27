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
 * rptchannel.h
 *    Runtime filter channel identities shared by both injection passes.
 *
 *-------------------------------------------------------------------------
 */
#ifndef RPTCHANNEL_H
#define RPTCHANNEL_H

#include "rptinject.h"

/*
 * Both ends of an exchange must copy the same ID and name. The ID comes
 * from one statement-wide counter, and Anser also uses the name to seed
 * Bloom hashing. Keep the forward and backward name formats distinct.
 */
inline void
RptAssignForwardChannel(RptInjectionState &state, RptForwardChannel &channel)
{
	channel.condition_key = "rpt_f_" + std::to_string(channel.source_rtindex) + "_" +
		std::to_string(channel.target_rtindex);
	channel.condition_id = state.next_condition_id++;
}

inline void
RptAssignBackwardChannel(RptInjectionState &state, RptBackwardClass &cls)
{
	cls.condition_key = "rpt_b_" + std::to_string(cls.producer_rtindex) + "_" +
		std::to_string(cls.class_key.rtindex) + "_" + std::to_string(cls.class_key.attno);
	cls.condition_id = state.next_condition_id++;
}

#endif /* RPTCHANNEL_H */
