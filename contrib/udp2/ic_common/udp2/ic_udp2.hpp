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
 * ic_udp2.hpp
 *
 * IDENTIFICATION
 *    contrib/udp2/ic_common/udp2/ic_udp2.hpp
 *
 *-------------------------------------------------------------------------
 */
#ifndef IC_UDP2_HPP
#define IC_UDP2_HPP

#include <map>
#include <memory>
#include <ostream>
#include <sstream>
#include <stdexcept>

#include "ic_types.h"
#include "ic_except.hpp"

struct CChunkTransportState : public ICChunkTransportState
{
public:
	static ICChunkTransportState* SetupUDP(ICSliceTable *sliceTable, SessionMotionLayerIPCParam *param);
	void TeardownUDP(bool hasErrors);

	void RecvRoute(int16 motNodeID, int16 srcRoute, GetDataLenInPacket getLen, DataBlock *data);
	void RecvAny(int16 motNodeID, int16 *srcRoute, GetDataLenInPacket getLen, DataBlock *data);
	void ReleaseAndAck(int motNodeID, int route);
	void SendStop(int16 motNodeID);
	void DeactiveRoute(int motNodeID, int srcRoute, const char *reason);

	void SendEOS(int motNodeID, DataBlock *data);
	bool SendData(int16 motNodeID, int16 targetRoute, DataBlock *pblocks, int num, bool broadcast);
	void GetFreeSpace(int16 motNodeID, int16 targetRoute, BufferBlock *b);
	void ReduceFreeSpace(int16 motNodeID, int16 targetRoute, int length);

	void* GetOpaqueDataInConn(int16 motNodeID, int16 targetRoute);
	int32* GetSentRecordTypmodInConn(int16 motNodeID, int16 targetRoute);

	int  GetConnNum(int motNodeID);

	static CChunkTransportState** GetTransportState();

	/* APIs for vector engine */
	void NotifyQuit();
	void SetVectorEngineAsUser();
};

#endif // IC_UDP2_HPP