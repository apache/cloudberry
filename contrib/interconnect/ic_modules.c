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
 * ic_modules.c
 *	   Interconnect code shared between UDP, and TCP IPC Layers.
 *
 * IDENTIFICATION
 *	    contrib/interconnect/ic_modules.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "ic_modules.h"
#include "ic_internal.h"
#include "ic_common.h"
#include "tcp/ic_tcp.h"
#include "udp/ic_udpifc.h"

#ifdef ENABLE_IC_PROXY
#include "proxy/ic_proxy_server.h"
#endif

PG_MODULE_MAGIC;

MotionIPCLayer tcp_ipc_layer = {
    .ic_type = INTERCONNECT_TYPE_TCP,
    .type_name = "tcp",

    .GetMaxTupleChunkSize = GetMaxTupleChunkSizeTCP,
    .GetListenPort = GetListenPortTCP,

    .InitMotionLayerIPC = InitMotionIPCLayerTCP,
    .CleanUpMotionLayerIPC = CleanUpMotionIPCLayerTCP,
    .WaitInterconnectQuit = WaitInterconnectQuitTCP,
    .SetupInterconnect = SetupInterconnectTCP,
    .TeardownInterconnect = TeardownInterconnectTCP,

    .SendTupleChunkToAMS = SendTupleChunkToAMS,
    .SendChunk = SendChunkTCP,
    .SendEOS = SendEOSTCP,
    .SendStopMessage = SendStopMessageTCP,

    .RecvTupleChunkFromAny = RecvTupleChunkFromAnyTCP,
    .RecvTupleChunkFrom = RecvTupleChunkFromTCP,
    .RecvTupleChunk = RecvTupleChunkTCP,

    .DirectPutRxBuffer = DirectPutRxBufferTCP,

    .DeregisterReadInterest = DeregisterReadInterestTCP,
    .GetActiveMotionConns = GetActiveMotionConnsTCP,

    .GetTransportDirectBuffer = GetTransportDirectBuffer,
    .PutTransportDirectBuffer = PutTransportDirectBuffer,
#ifdef ENABLE_IC_PROXY
    .IcProxyServiceMain = ic_proxy_server_main,
#else 
    .IcProxyServiceMain = NULL,
#endif

    .GetMotionConnTupleRemapper = GetMotionConnTupleRemapper,
};

MotionIPCLayer proxy_ipc_layer = {
    .ic_type = INTERCONNECT_TYPE_PROXY,
    .type_name = "proxy",

    .GetMaxTupleChunkSize = GetMaxTupleChunkSizeTCP,
    .GetListenPort = GetListenPortTCP,

    .InitMotionLayerIPC = InitMotionIPCLayerTCP,
    .CleanUpMotionLayerIPC = CleanUpMotionIPCLayerTCP,
    .WaitInterconnectQuit = WaitInterconnectQuitTCP,
    .SetupInterconnect = SetupInterconnectTCP,
    .TeardownInterconnect = TeardownInterconnectTCP,

    .SendTupleChunkToAMS = SendTupleChunkToAMS,
    .SendChunk = SendChunkTCP,
    .SendEOS = SendEOSTCP,
    .SendStopMessage = SendStopMessageTCP,

    .RecvTupleChunkFromAny = RecvTupleChunkFromAnyTCP,
    .RecvTupleChunkFrom = RecvTupleChunkFromTCP,
    .RecvTupleChunk = RecvTupleChunkTCP,

    .DirectPutRxBuffer = DirectPutRxBufferTCP,

    .DeregisterReadInterest = DeregisterReadInterestTCP,
    .GetActiveMotionConns = GetActiveMotionConnsTCP,

    .GetTransportDirectBuffer = GetTransportDirectBuffer,
    .PutTransportDirectBuffer = PutTransportDirectBuffer,
#ifdef ENABLE_IC_PROXY
    .IcProxyServiceMain = ic_proxy_server_main,
#else 
    .IcProxyServiceMain = NULL,
#endif

    .GetMotionConnTupleRemapper = GetMotionConnTupleRemapper,
};


MotionIPCLayer udpifc_ipc_layer = {
    .ic_type = INTERCONNECT_TYPE_UDPIFC,
    .type_name = "udpifc",

    .GetMaxTupleChunkSize = GetMaxTupleChunkSizeUDP,
    .GetListenPort = GetListenPortUDP,

    .InitMotionLayerIPC = InitMotionIPCLayerUDP,
    .CleanUpMotionLayerIPC = CleanUpMotionLayerIPCUDP,
    .WaitInterconnectQuit = WaitInterconnectQuitUDPIFC,
    .SetupInterconnect = SetupInterconnectUDP,
    .TeardownInterconnect = TeardownInterconnectUDP,

    .SendTupleChunkToAMS = SendTupleChunkToAMS,
    .SendChunk = SendChunkUDPIFC,
    .SendEOS = SendEOSUDPIFC,
    .SendStopMessage = SendStopMessageUDPIFC,

    .RecvTupleChunkFromAny = RecvTupleChunkFromAnyUDPIFC,
    .RecvTupleChunkFrom = RecvTupleChunkFromUDPIFC,
    .RecvTupleChunk = RecvTupleChunkUDPIFC,

    .DirectPutRxBuffer = MlPutRxBufferIFC,

    .DeregisterReadInterest = DeregisterReadInterestUDP,
    .GetActiveMotionConns = GetActiveMotionConnsUDPIFC,

    .GetTransportDirectBuffer = GetTransportDirectBuffer,
    .PutTransportDirectBuffer = PutTransportDirectBuffer,
#ifdef ENABLE_IC_PROXY
    .IcProxyServiceMain = ic_proxy_server_main,
#else 
    .IcProxyServiceMain = NULL,
#endif

    .GetMotionConnTupleRemapper = GetMotionConnTupleRemapper,
};

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress) {
        ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not load interconnect outside process shared preload")));
    }

	RegisterIPCLayerImpl(&tcp_ipc_layer);
	RegisterIPCLayerImpl(&udpifc_ipc_layer);
	RegisterIPCLayerImpl(&proxy_ipc_layer);
}
