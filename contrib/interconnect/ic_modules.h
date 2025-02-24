/*-------------------------------------------------------------------------
 *
 * ic_modules.h
 *	  Motion IPC Layer.
 *
 * IDENTIFICATION
 *		contrib/interconnect/ic_modules.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INTER_CONNECT_H
#define INTER_CONNECT_H

#include "cdb/ml_ipc.h"

extern MotionIPCLayer tcp_ipc_layer;
extern MotionIPCLayer proxy_ipc_layer;
extern MotionIPCLayer udpifc_ipc_layer;

extern void _PG_init(void);

#endif // INTER_CONNECT_H
