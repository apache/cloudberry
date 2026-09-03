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
 * dl_wrappers.h
 *	  The boundaries between C++ code and the PostgreSQL runtime.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/dl_wrappers.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DL_WRAPPERS_H
#define DL_WRAPPERS_H

#include <setjmp.h>

#include "common/dl_err.h"
#include "common/dl_pg_api.h"

/*
 * Exception-boundary classes:
 *
 * 1. PG-called C entry (extern "C" handler/UDF/hook):
 *    DL_TRY { } DL_CATCH_END(); converts C++ exceptions to ereport(ERROR,
 *    ...) at the boundary; no C++ exception may escape into PG stack frames.
 * 2. Status-returning DlErrCode ABI (vtable implementations): functions must
 *    be noexcept; DL_ABI_GUARD_BEGIN / DL_ABI_GUARD_END(errvar) converts every
 *    exception to DL_ERR_INTERNAL.
 * 3. Void cleanup ABI (close, abort, iterator close): functions must be
 *    noexcept and idempotent, and must never ereport.  The cleanup guard logs
 *    a best-effort WARNING only outside error cleanup and otherwise swallows.
 * 4. C++ calling PG APIs: DL_WRAP_START; ... DL_WRAP_END; converts a PG longjmp
 *    to DlPgError so the C++ caller can handle it without crossing the ABI.
 */

#ifdef __cplusplus

#include <exception>

class DlPgError : public std::exception {
public:
	const char *what() const noexcept override
	{
		return "PostgreSQL error";
	}
};

/* Save and restore PG's longjmp targets around a C++ call site. */
class DlPgExceptionStack {
public:
	DlPgExceptionStack(void **exception_stack, void **error_context_stack)
		: exception_stack_(exception_stack),
		  error_context_stack_(error_context_stack),
		  saved_exception_stack_(*exception_stack),
		  saved_error_context_stack_(*error_context_stack)
	{
	}

	~DlPgExceptionStack()
	{
		*exception_stack_ = saved_exception_stack_;
		*error_context_stack_ = saved_error_context_stack_;
	}

	void SetLocalJmp(void *local_jump)
	{
		*exception_stack_ = local_jump;
	}

private:
	void **exception_stack_;
	void **error_context_stack_;
	void *saved_exception_stack_;
	void *saved_error_context_stack_;
};

static inline bool
dl_can_log_cleanup_warning(void)
{
	return !in_error_recursion_trouble() && !IsAbortInProgress() &&
		!IsAbortedTransactionBlockState();
}

/*
 * Class 1: a C entry point called by PostgreSQL.
 *
 * ereport(ERROR) unwinds with longjmp(), and longjmp()ing out of a C++ catch
 * handler leaves the in-flight exception alive, which is undefined behavior.
 * So the handler only records what happened -- the message is copied into a
 * local buffer because the exception object dies with the handler -- and the
 * ereport() happens after the try/catch statement has been left, the same way
 * PAX defers it to CBDB_END_TRY().
 */
#define DL_ERROR_MSG_MAX 512

#define DL_TRY \
	do { \
		bool		dl_pending_error_ = false; \
		char		dl_error_msg_[DL_ERROR_MSG_MAX]; \
\
		dl_error_msg_[0] = '\0'; \
		try

#define DL_CATCH_END() \
		catch (const std::exception &e) \
		{ \
			dl_pending_error_ = true; \
			strlcpy(dl_error_msg_, e.what(), sizeof(dl_error_msg_)); \
		} \
		catch (...) \
		{ \
			dl_pending_error_ = true; \
			strlcpy(dl_error_msg_, "unknown C++ exception", \
					sizeof(dl_error_msg_)); \
		} \
		if (dl_pending_error_) \
			ereport(ERROR, \
					(errcode(ERRCODE_INTERNAL_ERROR), \
					 errmsg("datalake_fdw: %s", dl_error_msg_))); \
	} while (0)

#define DL_ABI_GUARD_BEGIN \
	try \
	{

#define DL_ABI_GUARD_END(errvar) \
	} \
	catch (...) \
	{ \
		(errvar) = DL_ERR_INTERNAL; \
	}

#define DL_CLEANUP_GUARD_BEGIN \
	do { \
		bool		dl_cleanup_failed_ = false; \
\
		try \
		{

/*
 * Class 3 cleanup guards must never ereport(), so the report is a WARNING at
 * most -- and it is emitted after the handler has been left, because even
 * elog() can escalate into an ERROR while the error subsystem is in trouble.
 */
#define DL_CLEANUP_GUARD_END \
	} \
	catch (...) \
	{ \
		dl_cleanup_failed_ = true; \
	} \
	if (dl_cleanup_failed_ && dl_can_log_cleanup_warning()) \
		elog(WARNING, "datalake_fdw: C++ exception during cleanup"); \
	} while (0)

/* Modeled on the PAX CBDB_WRAP_START/END saved-exception-stack pattern. */
#define DL_WRAP_START \
	sigjmp_buf dl_local_sigjmp_buf; \
	{ \
		DlPgExceptionStack dl_exception_stack( \
			reinterpret_cast<void **>(&PG_exception_stack), \
			reinterpret_cast<void **>(&error_context_stack)); \
		if (sigsetjmp(dl_local_sigjmp_buf, 0) == 0) \
		{ \
			dl_exception_stack.SetLocalJmp(&dl_local_sigjmp_buf)

#define DL_WRAP_END \
		} \
		else \
		{ \
			throw DlPgError(); \
		} \
	}

#endif						/* __cplusplus */

#endif						/* DL_WRAPPERS_H */
