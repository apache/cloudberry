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
 * ic_utility.hpp
 *
 * IDENTIFICATION
 *    contrib/udp2/ic_common/ic_utility.hpp
 *
 *-------------------------------------------------------------------------
 */
#ifndef IC_UTILITY_HPP
#define IC_UTILITY_HPP

#include <atomic>
#include <mutex>
#include <sstream>
#include <vector>

#include <stdarg.h>
#include <sys/time.h>
#include <unistd.h>

/* Define this if you want tons of logs! */
#undef AMS_VERBOSE_LOGGING

#ifndef NDEBUG
#define USE_ASSERT_CHECKING 1
#endif

#define Assert(p) assert(p)
#define closesocket close

#define IC_DEBUG5		10
#define IC_DEBUG4		11
#define IC_DEBUG3		12
#define IC_DEBUG2		13
#define IC_DEBUG1		14
#define IC_LOG			15
#define IC_WARNING		19

/* tupchunk.h */
#define ANY_ROUTE -100

/* cdbvars.h */
#define DEFAULT_PACKET_SIZE 8192
#define MIN_PACKET_SIZE 512
#define MAX_PACKET_SIZE 65507 /* Max payload for IPv4/UDP (subtract 20 more for IPv6 without extensions) */
#define UNDEF_SEGMENT -2

/* c.h */
#define Max(x, y)		((x) > (y) ? (x) : (y))
#define Min(x, y)		((x) < (y) ? (x) : (y))

/* cdbinterconnect.h */
#define CTS_INITIAL_SIZE (10)

/* pg_list.h */
#define NIL						((List *) NULL)

/* transam.h */
#define InvalidTransactionId		(0)

#define INVALID_SOCKET (-1)

/*
 * CONTAINER_OF
 */
#define CONTAINER_OF(ptr, type, member) \
	( \
	  reinterpret_cast<type*>(reinterpret_cast<char*>(ptr) - offsetof(type, member)) \
	)


typedef enum GpVars_Verbosity_IC
{
	GPVARS_VERBOSITY_UNDEFINED_IC = 0,
	GPVARS_VERBOSITY_OFF_IC,
	GPVARS_VERBOSITY_TERSE_IC,
	GPVARS_VERBOSITY_VERBOSE_IC,
	GPVARS_VERBOSITY_DEBUG_IC,
} GpVars_Verbosity_IC;

typedef enum GpVars_Interconnect_Method_IC
{
	INTERCONNECT_FC_METHOD_CAPACITY_IC = 0,
	INTERCONNECT_FC_METHOD_LOSS_IC = 2,
} GpVars_Interconnect_Method_IC;

typedef enum
{
	GP_ROLE_UNDEFINED_IC = 0,		/* Should never see this role in use */
	GP_ROLE_UTILITY_IC,			/* Operating as a simple database engine */
	GP_ROLE_DISPATCH_IC,			/* Operating as the parallel query dispatcher */
	GP_ROLE_EXECUTE_IC,			/* Operating as a parallel query executor */
} GpRoleValue_IC;

typedef enum GpVars_Interconnect_Address_Type_IC
{
	INTERCONNECT_ADDRESS_TYPE_UNICAST_IC = 0,
	INTERCONNECT_ADDRESS_TYPE_WILDCARD_IC
} GpVars_Interconnect_Address_Type_IC;


/*
 * global_param and session_param;
 */
extern GlobalMotionLayerIPCParam global_param;
extern SessionMotionLayerIPCParam session_param;

/*
 * logger stuff
 */
#define DEFAULT_LOG_LEVEL INFO
extern const char * SeverityName[9];

enum LogSeverity {
    FATAL, LOG_ERROR, WARNING, INFO, DEBUG1, DEBUG2, DEBUG3, DEBUG4, DEBUG5
};

class Logger;

class Logger {
public:
    Logger();

    ~Logger();

    void setOutputFd(int f);

    void setLogSeverity(LogSeverity l);

    void printf(LogSeverity s, const char * fmt, ...) __attribute__((format(printf, 3, 4)));

private:
    int fd;
    std::atomic<LogSeverity> severity;
};

extern Logger RootLogger;

#define LOG(s, fmt, ...) \
    RootLogger.printf(s, fmt, ##__VA_ARGS__)

/*
 * crc32
 */
extern uint32 ComputeCRC(const void *data, size_t len);

#endif // IC_UTILITY_HPP