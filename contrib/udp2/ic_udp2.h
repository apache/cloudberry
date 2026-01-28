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
 *    contrib/udp2/ic_udp2.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef IC_UDP_H
#define IC_UDP_H

#include "cdb/cdbinterconnect.h"
#include "nodes/execnodes.h"	/* EState, ExecSlice, SliceTable */

extern int GetMaxTupleChunkSizeUDP2(void);
extern int32 GetListenPortUDP2(void);

extern void InitMotionIPCLayerUDP2(void);
extern void CleanUpMotionLayerIPCUDP2(void);

extern void WaitInterconnectQuitUDPIFC2(void);

extern void SetupInterconnectUDP2(EState *estate);
extern void TeardownInterconnectUDP2(ChunkTransportState * transportStates, bool hasErrors);

extern bool SendTupleChunkToAMSUDP2(ChunkTransportState *transportStates,
									int16 motNodeID,
									int16 targetRoute,
									TupleChunkListItem tcItem);
extern void SendEOSUDPIFC2(ChunkTransportState * transportStates,
						   int motNodeID, TupleChunkListItem tcItem);
extern void SendStopMessageUDPIFC2(ChunkTransportState * transportStates, int16 motNodeID);

extern TupleChunkListItem RecvTupleChunkFromAnyUDPIFC2(ChunkTransportState * transportStates,
													   int16 motNodeID,
													   int16 *srcRoute);
extern TupleChunkListItem RecvTupleChunkFromUDPIFC2(ChunkTransportState * transportStates,
													int16 motNodeID,
													int16 srcRoute);

void MlPutRxBufferUDPIFC2(ChunkTransportState * transportStates, int motNodeID, int route);

extern void DeregisterReadInterestUDP2(ChunkTransportState * transportStates,
									   int motNodeID,
									   int srcRoute,
									   const char *reason);

extern uint32 GetActiveMotionConnsUDPIFC2(void);

extern void GetTransportDirectBufferUDPIFC2(ChunkTransportState *transportStates,
											int16 motNodeID,
											int16 targetRoute,
											struct directTransportBuffer *b);
extern void PutTransportDirectBufferUDPIFC2(ChunkTransportState *transportStates,
											int16 motNodeID,
											int16 targetRoute,
											int length);

extern TupleRemapper* GetMotionConnTupleRemapperUDPIFC2(ChunkTransportState *transportStates,
														int16 motNodeID,
														int16 targetRoute);

extern int32* GetMotionSentRecordTypmodUDPIFC2(ChunkTransportState * transportStates,
								 				int16 motNodeID,
								 				int16 targetRoute);

extern int ic_proxy_server_main(void);

#endif // IC_UDP_H