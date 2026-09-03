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
 * UDSConnector.cpp
 *
 * IDENTIFICATION
 *	  gpcontrib/gp_stats_collector/src/UDSConnector.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "UDSConnector.h"
#include "Config.h"
#include "GpscStat.h"
#include "memory/gpdbwrappers.h"
#include "pg_query_state/qs_types.h"

#include <string>
#include <sys/fcntl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

extern "C" {
#include "postgres.h"
}

static void inline log_tracing_failure(const gpsc::SetQueryReq &req,
									   const std::string &event)
{
	ereport(LOG, (errmsg("Query {%d-%d-%d} %s tracing failed with error %m",
						 req.query_key().tmid(), req.query_key().ssid(),
						 req.query_key().ccnt(), event.c_str())));
}

bool
UDSConnector::report_query(const gpsc::SetQueryReq &req,
						   const std::string &event, const Config &config)
{
	sockaddr_un address{};
	address.sun_family = AF_UNIX;
	const auto &uds_path = config.uds_path();

	if (uds_path.size() >= sizeof(address.sun_path))
	{
		ereport(WARNING, (errmsg("UDS path is too long for socket buffer")));
		GpscStat::report_error();
		return false;
	}
	strcpy(address.sun_path, uds_path.c_str());

	const auto sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (sockfd == -1)
	{
		log_tracing_failure(req, event);
		GpscStat::report_error();
		return false;
	}

	// Close socket automatically on error path.
	struct SockGuard
	{
		int fd;
		~SockGuard()
		{
			close(fd);
		}
	} sock_guard{sockfd};

	if (fcntl(sockfd, F_SETFL, O_NONBLOCK) == -1)
	{
		// That's a very important error that should never happen, so make it
		// visible to an end-user and admins.
		ereport(WARNING,
				(errmsg("Unable to create non-blocking socket connection %m")));
		GpscStat::report_error();
		return false;
	}

	if (connect(sockfd, reinterpret_cast<sockaddr *>(&address),
				sizeof(address)) == -1)
	{
		log_tracing_failure(req, event);
		GpscStat::report_bad_connection();
		return false;
	}

	const auto data_size = req.ByteSizeLong();
	const auto total_size = data_size + sizeof(uint32_t);
	auto *buf = static_cast<uint8_t *>(gpdb::palloc(total_size));
	// Free buf automatically on error path.
	struct BufGuard
	{
		void *p;
		~BufGuard()
		{
			gpdb::pfree(p);
		}
	} buf_guard{buf};

	*reinterpret_cast<uint32_t *>(buf) = data_size;
	req.SerializeWithCachedSizesToArray(buf + sizeof(uint32_t));

	int64_t sent = 0, sent_total = 0;
	do
	{
		sent = send(sockfd, buf + sent_total, total_size - sent_total,
					MSG_DONTWAIT);
		if (sent > 0)
			sent_total += sent;
	} while (sent > 0 && size_t(sent_total) != total_size &&
			 // the line below is a small throttling hack:
			 // if a message does not fit a single packet, we take a nap
			 // before sending the next one.
			 // Otherwise, MSG_DONTWAIT send might overflow the UDS
			 (pg_usleep(1000), true));

	if (sent < 0)
	{
		log_tracing_failure(req, event);
		GpscStat::report_bad_send(total_size);
		return false;
	}

	GpscStat::report_send(total_size);
	return true;
}

// Extended protocol used by the runtime query-state messages. The high bit of
// the size word tells the receiver that an 8-byte header follows instead of the
// original 4-byte one; the request type word then selects the payload message.
static const uint32_t kExtendedProtocolFlag = 0x80000000u;
static const uint16_t kRequestTypePerNodeBatch = 1;
static const uint16_t kRequestTypeQueryPlan = 2;

static void inline log_tracing_failure(const yagpcc::SetPerNodeBatchReq &req)
{
	static const char hexchars[] = "0123456789abcdef";
	const unsigned char *trace_id =
		reinterpret_cast<const unsigned char *>(req.trace_id().data());
	size_t len = req.trace_id().size();
	char hex[GPSC_TRACE_ID_LEN * 2 + 1];

	if (len > GPSC_TRACE_ID_LEN)
		len = GPSC_TRACE_ID_LEN;

	for (size_t i = 0; i < len; ++i)
	{
		hex[i * 2] = hexchars[trace_id[i] >> 4];
		hex[i * 2 + 1] = hexchars[trace_id[i] & 0x0f];
	}
	hex[len * 2] = '\0';

	ereport(LOG,
			(errmsg("Per-node batch {%s} tracing of %d nodes failed with error %m",
					hex, req.nodes_size())));
}

static void inline log_tracing_failure(const yagpcc::SetQueryPlanReq &req)
{
	ereport(LOG,
			(errmsg("Query {%d-%d-%d} plan document of %zu bytes failed with error %m",
					req.query_key().tmid(), req.query_key().ssid(),
					req.query_key().ccnt(), req.plan_doc().size())));
}

// Sends req behind the 8-byte extended header. Delivery repeats report_query()
// above: a fresh non-blocking socket per message, MSG_DONTWAIT, a nap between
// packets, and a plain drop once the socket refuses to take more. We never wait
// on the reader -- a stalled yagpcc must not slow down the query it observes.
//
// The template exists so that log_tracing_failure() resolves by overload while
// still being called before ~SockGuard() closes the socket and clobbers errno.
template <typename Req>
static bool
report_extended(const Req &req, uint16_t request_type, const Config &config)
{
	sockaddr_un address{};
	address.sun_family = AF_UNIX;
	const auto &uds_path = config.uds_path();

	if (uds_path.size() >= sizeof(address.sun_path))
	{
		ereport(WARNING, (errmsg("UDS path is too long for socket buffer")));
		GpscStat::report_error();
		return false;
	}
	strcpy(address.sun_path, uds_path.c_str());

	const auto sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (sockfd == -1)
	{
		log_tracing_failure(req);
		GpscStat::report_error();
		return false;
	}

	// Close socket automatically on error path.
	struct SockGuard
	{
		int fd;
		~SockGuard()
		{
			close(fd);
		}
	} sock_guard{sockfd};

	if (fcntl(sockfd, F_SETFL, O_NONBLOCK) == -1)
	{
		// That's a very important error that should never happen, so make it
		// visible to an end-user and admins.
		ereport(WARNING,
				(errmsg("Unable to create non-blocking socket connection %m")));
		GpscStat::report_error();
		return false;
	}

	if (connect(sockfd, reinterpret_cast<sockaddr *>(&address),
				sizeof(address)) == -1)
	{
		log_tracing_failure(req);
		GpscStat::report_bad_connection();
		return false;
	}

	const auto data_size = req.ByteSizeLong();
	const auto header_size = sizeof(uint32_t) + 2 * sizeof(uint16_t);
	const auto total_size = data_size + header_size;
	auto *buf = static_cast<uint8_t *>(gpdb::palloc(total_size));
	struct BufGuard
	{
		void *p;
		~BufGuard()
		{
			gpdb::pfree(p);
		}
	} buf_guard{buf};

	*reinterpret_cast<uint32_t *>(buf) =
		static_cast<uint32_t>(data_size) | kExtendedProtocolFlag;
	*reinterpret_cast<uint16_t *>(buf + sizeof(uint32_t)) = request_type;
	*reinterpret_cast<uint16_t *>(buf + sizeof(uint32_t) + sizeof(uint16_t)) = 0;
	req.SerializeWithCachedSizesToArray(buf + header_size);

	int64_t sent = 0, sent_total = 0;
	do
	{
		sent = send(sockfd, buf + sent_total, total_size - sent_total,
					MSG_DONTWAIT);
		if (sent > 0)
			sent_total += sent;
	} while (sent > 0 && size_t(sent_total) != total_size &&
			 (pg_usleep(1000), true));

	if (sent < 0)
	{
		log_tracing_failure(req);
		GpscStat::report_bad_send(total_size);
		return false;
	}

	GpscStat::report_send(total_size);
	return true;
}

bool
UDSConnector::report_per_node_batch(const yagpcc::SetPerNodeBatchReq &req,
									const Config &config)
{
	return report_extended(req, kRequestTypePerNodeBatch, config);
}

bool
UDSConnector::report_query_plan(const yagpcc::SetQueryPlanReq &req,
								const Config &config)
{
	return report_extended(req, kRequestTypeQueryPlan, config);
}
