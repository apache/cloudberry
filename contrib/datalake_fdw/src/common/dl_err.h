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
 * dl_err.h
 *	  Error codes shared by the layers below the access method, and the
 *	  channel that carries what the code alone cannot say.
 *
 * A code says which kind of failure occurred.  It cannot say which table the
 * remote catalog rejected, what the storage service answered, or where a remote
 * implementation threw -- and those are the only things that make such a failure
 * diagnosable.  Every layer below the access method therefore reports a code and
 * additionally records the detail here; the entry points that face PostgreSQL
 * turn both into one ereport.
 *
 * The detail is recorded into fixed-size storage on purpose.  Recording happens
 * on paths that must not allocate and must not raise -- a cleanup callback
 * crossing back from C++ is one of them -- so a setter that could palloc, and
 * therefore could fail, is not usable there.  Long values are truncated, which
 * is the right trade against losing the report entirely.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/dl_err.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DL_ERR_H
#define DL_ERR_H

#ifdef __cplusplus
extern "C" {
#endif

typedef enum DlErrCode {
	DL_OK = 0,
	DL_ERR_NOT_SUPPORTED,
	DL_ERR_INVALID_OPTION,
	DL_ERR_NOT_FOUND,
	DL_ERR_ALREADY_EXISTS,
	DL_ERR_IO,
	DL_ERR_INTERNAL,
} DlErrCode;

#define DL_ERR_FIELD_LEN	128
#define DL_ERR_MSG_LEN		1024
#define DL_ERR_STACK_LEN	4096

/*
 * What an implementation below the access method has to say about its last
 * failure.  The field set follows what a remote metadata engine reports, so
 * that connecting one is a matter of filling this in rather than changing it.
 */
typedef struct DlErrorDetail
{
	DlErrCode	code;
	int			remote_code;			/* implementation's own numeric code, 0
										 * when it has none */
	char		operation[DL_ERR_FIELD_LEN];	/* which call failed */
	char		type[DL_ERR_FIELD_LEN]; /* implementation's error class */
	char		message[DL_ERR_MSG_LEN];
	char		stack[DL_ERR_STACK_LEN];
} DlErrorDetail;

/*
 * Discard any recorded detail.  Called by the dispatch wrappers before entering
 * an implementation, so that a report can never describe an earlier failure.
 */
extern void dl_error_reset(void);

/*
 * Record the detail for a failure that is being reported as `code`.  Allocates
 * nothing and raises nothing, so it is callable from a cleanup path.  NULL is
 * accepted for any string and leaves that field empty.
 */
extern void dl_error_set(DlErrCode code, const char *operation,
						 const char *type, const char *message);

/* Record the implementation's own numeric code, when it reports one. */
extern void dl_error_set_remote_code(int remote_code);

/* Record a stack from the failing implementation. */
extern void dl_error_set_stack(const char *stack);

/* The recorded detail, never NULL; code is DL_OK when nothing was recorded. */
extern const DlErrorDetail *dl_error_get(void);

/* Short, code-only wording, for when there is nothing recorded to add. */
extern const char *dl_err_message(DlErrCode code);

/*
 * Report a failure to PostgreSQL: `prefix` names what was being attempted, the
 * recorded message becomes the detail, and a recorded stack is included only
 * when the session asked for log-level detail -- a stack is for whoever is
 * debugging the implementation, not for whoever ran the statement.
 *
 * Detail recorded against a different code is ignored rather than misattributed,
 * and reporting consumes what it used.  Matching on the code alone cannot tell
 * this failure's detail from an earlier failure's with the same code, so the
 * record is not left behind for the next one to inherit.
 */
extern void dl_error_report(int elevel, DlErrCode code, const char *prefix);

/*
 * An argument that was not what it had to be.  A caller reporting the code
 * alone would say "invalid parameter" about a call the user never wrote, so
 * this names the entry point instead -- there is no user error to describe,
 * only which one of ours was called wrongly.
 */
#define DL_ARG_ERROR(operation) \
	(dl_error_set(DL_ERR_INVALID_OPTION, (operation), NULL, \
				  "a required argument was missing"), DL_ERR_INVALID_OPTION)

#ifdef __cplusplus
}
#endif

#endif						/* DL_ERR_H */
