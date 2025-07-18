#ifndef GREENGAGE_GREENGAGE_CLUSTER_INFO_INTERNAL_H
#define GREENGAGE_GREENGAGE_CLUSTER_INFO_INTERNAL_H
/*
 *	greengage/greengage_cluster_info_internal.h
 *
 *	Portions Copyright (c) 2019-Present, Pivotal Software Inc
 *	contrib/pg_upgrade/greengage/greengage_cluster_info_internal.h
 */

#include "postgres_fe.h"
#include "greengage_cluster_info.h"

GreengageClusterInfo *make_cluster_info(void);

int get_gp_dbid(GreengageClusterInfo *info);

void set_gp_dbid(GreengageClusterInfo *info, int gp_dbid);

bool is_gp_dbid_set(GreengageClusterInfo *info);

#endif
