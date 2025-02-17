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
 * etcdutils.h
 *	  Interface for etcd configuration file parsing tools.
 *
 * src/include/common/etcdutils.h
 * 
 *-------------------------------------------------------------------------
 */
#ifndef ETCDUTILS_H
#define ETCDUTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdbool.h>
#include "postgres.h"
#include "postmaster/fts_comm.h"
#include "utils/memutils.h"

#define FTS_METADATA_DIR_PREFIX "/cbdb/fts"
#define GP_ETCD_KEY_LEN 256
#define GP_ETCD_HOSTNAME_LEN 512
#define GP_ETCD_PORT_LEN 32
#define GPETCDCONFIGNUMATTR 2
#define GP_ETCD_SEPERATOR ","
#define GP_ETCD_ENDPOINTS_NUM 32
#define GP_ETCD_NAMESPACE_DEFAULT "default"
#define GP_ETCD_ACCOUNT_ID_DEFAULT "00000000-0000-0000-0000-000000000000"
#define GP_ETCD_CLUSTER_ID_DEFAULT "00000000-0000-0000-0000-000000000000"
#define GP_ETCD_ENDPOINTS_DEFAULT "localhost:2379"

typedef struct etcdlib_endpoint {
    char *etcd_host;
    int etcd_port;
} etcdlib_endpoint_t;

extern bool generateGPSegConfigKey(char *fts_dump_file_key, const char *gp_etcd_namespace, const char *gp_etcd_account_id,
						const char *gp_etcd_cluster_id, char *gp_etcd_endpoints, etcdlib_endpoint_t *etcd_endpoints, int *etcd_endpoints_num);

extern void generateGPFtsPromoteReadyKey(char *fts_standby_promote_ready_key, const char *gp_etcd_namespace, const char *gp_etcd_account_id, const char *gp_etcd_cluster_id);

#endif

