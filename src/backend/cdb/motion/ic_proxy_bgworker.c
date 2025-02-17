/*-------------------------------------------------------------------------
 *
 * ic_proxy_bgworker.c
 *
 *    Interconnect Proxy Background Worker
 *
 * This is only a wrapper, the actual main loop is in ic_proxy_main.c .
 *
 *
 * Copyright (c) 2020-Present VMware, Inc. or its affiliates.
 *
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "storage/ipc.h"
#include "cdb/ic_proxy_bgworker.h"
#include "cdb/ml_ipc.h"

bool
ICProxyStartRule(Datum main_arg)
{
	return true;
}

/*
 * ICProxyMain
 */
void
ICProxyMain(Datum main_arg)
{
	/* in utility mode, won't preload interconnect module. 
	 * also won't call cdb_setup().
	 */ 
	if (CurrentMotionIPCLayer) {
		proc_exit(CurrentMotionIPCLayer->IcProxyServiceMain());
	} else {
		proc_exit(0);
	}
}
