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
 * ic_udp2.h
 *
 * IDENTIFICATION
 *    contrib/udp2/ic_common/udp2/ic_udp2.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef IC_UDP2_H
#define IC_UDP2_H

#include "ic_types.h"

#ifdef __cplusplus
extern "C" {
#endif

extern void UDP2_InitUDPIFC(struct GlobalMotionLayerIPCParam *param);
extern void UDP2_CleanUpUDPIFC(void);
extern void UDP2_WaitQuitUDPIFC(void);

extern ICChunkTransportState* UDP2_SetupUDP(ICSliceTable *sliceTable,
											SessionMotionLayerIPCParam *param);
extern void UDP2_TeardownUDP(ICChunkTransportState *transportStates,
							 bool hasErrors);

// recv
extern void UDP2_RecvRoute(ICChunkTransportState *transportStates,
						   int16 motNodeID,
						   int16 srcRoute,
						   GetDataLenInPacket getLen,
						   DataBlock *data);
extern void UDP2_RecvAny(ICChunkTransportState *transportStates,
						 int16 motNodeID,
						 int16 *srcRoute,
						 GetDataLenInPacket getLen,
						 DataBlock *data);
extern void UDP2_SendStop(ICChunkTransportState *transportStates, int16 motNodeID);
extern void UDP2_ReleaseAndAck(ICChunkTransportState *transportStates,
							   int motNodeID,
							   int route);
extern void UDP2_DeactiveRoute(ICChunkTransportState *transportStates,
							   int motNodeID,
							   int srcRoute,
							   const char *reason);

// send
extern void UDP2_SendEOS(ICChunkTransportState *transportStates,
						 int motNodeID,
						 DataBlock *data);
extern bool UDP2_SendData(ICChunkTransportState *transportStates,
						  int16 motNodeID,
						  int16 targetRoute,
						  DataBlock *pblocks,
						  int num,
						  bool broadcast);
extern void UDP2_GetFreeSpace(ICChunkTransportState *transportStates,
							  int16 motNodeID,
							  int16 targetRoute,
							  BufferBlock *b);
extern void UDP2_ReduceFreeSpace(ICChunkTransportState *transportStates,
								 int16 motNodeID,
								 int16 targetRoute,
								 int length);


// utility func
extern void* UDP2_GetOpaqueDataInConn(ICChunkTransportState *transportStates,
									  int16 motNodeID,
									  int16 targetRoute);
extern int32* UDP2_GetSentRecordTypmodInConn(ICChunkTransportState *transportStates,
									  int16 motNodeID,
									  int16 targetRoute);

extern uint32 UDP2_GetActiveConns(void);
extern int    UDP2_GetICHeaderSizeUDP(void);
extern int32  UDP2_GetListenPortUDP(void);

#ifdef __cplusplus
}
#endif

#endif // IC_UDP2_H