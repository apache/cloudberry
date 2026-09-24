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
 * dl_err.c
 *	  Error codes shared by the layers below the access method, and the
 *	  channel that carries what the code alone cannot say.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/dl_err.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "utils/memutils.h"

#include "common/dl_err.h"
#include "utils/guc.h"

/*
 * One record per backend.  A backend runs one statement at a time, and nothing
 * here outlives the report it feeds, so there is no reason to key this by
 * anything finer.
 */
static DlErrorDetail dl_error_detail;

static void dl_error_copy_field(char *dest, Size dest_size, const char *src);
static int	dl_error_sqlstate(DlErrCode code);

/*
 * SQLSTATE for a failure below the access method.
 *
 * Only a defect in this module is an internal error.  A remote catalog that
 * refuses a name, or storage that will not answer, is a condition a client can
 * act on and deserves a code that says so -- and reporting everything as
 * ERRCODE_INTERNAL_ERROR has a second cost here: this server appends the
 * raising source location to internal errors, which puts a file and line number
 * into user-visible output and into every expected-output file.
 */
static int
dl_error_sqlstate(DlErrCode code)
{
	switch (code)
	{
		case DL_OK:
		case DL_ERR_INTERNAL:
			return ERRCODE_INTERNAL_ERROR;
		case DL_ERR_NOT_SUPPORTED:
			return ERRCODE_FEATURE_NOT_SUPPORTED;
		case DL_ERR_INVALID_OPTION:
			return ERRCODE_INVALID_PARAMETER_VALUE;
		case DL_ERR_NOT_FOUND:
			return ERRCODE_UNDEFINED_OBJECT;
		case DL_ERR_ALREADY_EXISTS:
			return ERRCODE_DUPLICATE_TABLE;
		case DL_ERR_IO:
			return ERRCODE_IO_ERROR;
		case DL_ERR_OUT_OF_MEMORY:
			return ERRCODE_OUT_OF_MEMORY;
	}

	return ERRCODE_INTERNAL_ERROR;
}

static void
dl_error_copy_field(char *dest, Size dest_size, const char *src)
{
	if (src == NULL)
	{
		dest[0] = '\0';
		return;
	}

	/* Truncates rather than failing; see the header for why. */
	strlcpy(dest, src, dest_size);
}

/*
 * Values that must never appear in what a user or a log is shown.
 *
 * Credentials arrive as options and end up inside a backend's client, from
 * where any number of things can quote them back: an SDK message, an
 * exception's what(), a third-party backend's own wording.  Scrubbing at each
 * of those places means every one of them has to remember to; scrubbing here,
 * where every error is recorded, means none of them has to.
 *
 * The list only grows.  A session that has mounted a volume keeps hiding that
 * volume's secrets afterwards, which is the safe direction to be wrong in,
 * and it stays small because it holds credentials rather than data.
 *
 * Nothing about a value disqualifies it.  There is no minimum length and no
 * ceiling on how many are held: a short credential makes for noisy messages,
 * because ordinary words get masked along with it, and that is the price of
 * the rule holding without exception.  A value declined here would be one
 * this code knows about and prints anyway.
 */
static char **dl_secrets;
static int	dl_nsecrets;
static int	dl_secrets_size;

/*
 * Set when a credential could not be recorded at all.  Redaction then has a
 * hole this code cannot see, so messages are withheld rather than shown --
 * the only thing worse than an unhelpful error is one that prints a key.
 */
static bool dl_redaction_incomplete;

void
dl_error_add_secret(const char *value)
{
	int			i;
	char	   *copy;

	if (value == NULL || value[0] == '\0')
		return;

	for (i = 0; i < dl_nsecrets; i++)
	{
		if (strcmp(dl_secrets[i], value) == 0)
			return;
	}

	/*
	 * malloc rather than palloc: this is reached from inside C++ frames, and
	 * an ereport out of a failed allocation would longjmp past their
	 * destructors.  A failure is recorded here and answered by withholding
	 * messages, not raised.
	 */
	if (dl_nsecrets == dl_secrets_size)
	{
		int			want = dl_secrets_size == 0 ? 16 : dl_secrets_size * 2;
		char	  **grown = (char **) realloc(dl_secrets,
											  (Size) want * sizeof(char *));

		if (grown == NULL)
		{
			dl_redaction_incomplete = true;
			return;
		}
		dl_secrets = grown;
		dl_secrets_size = want;
	}

	copy = strdup(value);
	if (copy == NULL)
	{
		dl_redaction_incomplete = true;
		return;
	}
	dl_secrets[dl_nsecrets++] = copy;
}

/*
 * Copy src into a fixed-size field, replacing every secret with "***" as it
 * goes.
 *
 * Matching is done against src rather than against what has already been
 * copied.  Scrubbing a field after filling it would be too late: the field
 * truncates, and a key that straddles the end would leave its first half
 * behind with nothing left to match.  Looking ahead in the source means a
 * secret is recognised before any of it is written, and if even "***" no
 * longer fits the copy simply stops.
 */
static void
dl_error_copy_scrubbed(char *dest, Size dest_size, const char *src)
{
	Size		out = 0;

	if (src == NULL)
	{
		dest[0] = '\0';
		return;
	}

	while (*src != '\0' && out + 1 < dest_size)
	{
		Size		match = 0;
		int			i;

		/*
		 * The longest secret that matches here, not the first: one secret can
		 * be a prefix of another -- a rotated key, two volumes -- and stopping
		 * at the shorter would copy the rest of the longer one out.
		 */
		for (i = 0; i < dl_nsecrets; i++)
		{
			Size		len = strlen(dl_secrets[i]);

			if (len > match && strncmp(src, dl_secrets[i], len) == 0)
				match = len;
		}

		if (match > 0)
		{
			if (out + 3 + 1 > dest_size)
				break;
			memcpy(dest + out, "***", 3);
			out += 3;
			src += match;
		}
		else
			dest[out++] = *src++;
	}

	dest[out] = '\0';
}

void
dl_error_reset(void)
{
	dl_error_detail.code = DL_OK;
	dl_error_detail.remote_code = 0;
	dl_error_detail.operation[0] = '\0';
	dl_error_detail.type[0] = '\0';
	dl_error_detail.message[0] = '\0';
	dl_error_detail.stack[0] = '\0';
}

void
dl_error_set(DlErrCode code, const char *operation, const char *type,
			 const char *message)
{
	dl_error_detail.code = code;
	dl_error_detail.remote_code = 0;
	dl_error_copy_field(dl_error_detail.operation,
						sizeof(dl_error_detail.operation), operation);
	/* Whatever produced these, they are about to be shown to somebody. */
	if (dl_redaction_incomplete)
	{
		/*
		 * A credential is missing from the list, so nothing that came from a
		 * service or a backend can be shown: it may hold that credential and
		 * there is no way left to tell.
		 */
		dl_error_detail.type[0] = '\0';
		strlcpy(dl_error_detail.message,
				"the message was withheld: a credential could not be recorded "
				"for redaction",
				sizeof(dl_error_detail.message));
	}
	else
	{
		dl_error_copy_scrubbed(dl_error_detail.type,
							   sizeof(dl_error_detail.type), type);
		dl_error_copy_scrubbed(dl_error_detail.message,
							   sizeof(dl_error_detail.message), message);
	}
	dl_error_detail.stack[0] = '\0';
}

void
dl_error_set_remote_code(int remote_code)
{
	dl_error_detail.remote_code = remote_code;
}

void
dl_error_set_stack(const char *stack)
{
	dl_error_copy_scrubbed(dl_error_detail.stack,
						   sizeof(dl_error_detail.stack), stack);
}

const DlErrorDetail *
dl_error_get(void)
{
	return &dl_error_detail;
}

const char *
dl_err_message(DlErrCode code)
{
	switch (code)
	{
		case DL_OK:
			return "success";
		case DL_ERR_NOT_SUPPORTED:
			return "operation not supported";
		case DL_ERR_INVALID_OPTION:
			return "invalid option";
		case DL_ERR_NOT_FOUND:
			return "not found";
		case DL_ERR_ALREADY_EXISTS:
			return "already exists";
		case DL_ERR_IO:
			return "I/O error";
		case DL_ERR_INTERNAL:
			return "internal error";
		case DL_ERR_OUT_OF_MEMORY:
			return "out of memory";
	}

	return "unknown error";
}

void
dl_error_report(int elevel, DlErrCode code, const char *prefix)
{
	/*
	 * A copy, because reporting consumes the record: what is left otherwise is
	 * a description of a failure that has already been reported, waiting for
	 * the next failure with the same code to adopt it -- and the check below
	 * cannot tell those two apart.  The reset has to happen before the ereport,
	 * which at ERROR does not come back.
	 */
	DlErrorDetail detail_copy = *dl_error_get();
	const DlErrorDetail *detail = &detail_copy;
	StringInfoData detail_buf;
	bool		has_detail;

	dl_error_reset();

	/*
	 * Code below C cannot service an interrupt, so it stops and fails instead
	 * (an S3 listing does).  Taking a pending one here first reports the
	 * cancel the user asked for rather than the failure it caused.
	 */
	if (elevel >= ERROR)
		CHECK_FOR_INTERRUPTS();

	/*
	 * Detail recorded against a different code belongs to some other failure --
	 * an implementation that reported this one without recording anything, for
	 * instance.  Reporting it here would attribute the wrong cause.
	 */
	has_detail = (detail->code == code &&
				  (detail->message[0] != '\0' ||
				   detail->type[0] != '\0' ||
				   detail->remote_code != 0));

	if (!has_detail)
	{
		ereport(elevel,
				(errcode(dl_error_sqlstate(code)),
				 errmsg("iceberg: %s failed: %s", prefix,
						dl_err_message(code))));
		return;
	}

	initStringInfo(&detail_buf);

	if (detail->operation[0] != '\0')
		appendStringInfo(&detail_buf, "%s: ", detail->operation);

	if (detail->message[0] != '\0')
		appendStringInfoString(&detail_buf, detail->message);
	else
		appendStringInfoString(&detail_buf, dl_err_message(code));

	if (detail->type[0] != '\0')
		appendStringInfo(&detail_buf, " (%s", detail->type);
	if (detail->remote_code != 0)
		appendStringInfo(&detail_buf, "%s%d",
						 detail->type[0] != '\0' ? ", code " : " (code ",
						 detail->remote_code);
	if (detail->type[0] != '\0' || detail->remote_code != 0)
		appendStringInfoChar(&detail_buf, ')');

	/*
	 * A stack describes the implementation, not the statement, so it is offered
	 * only to a session that asked to see log-level detail.
	 */
	if (detail->stack[0] != '\0' && client_min_messages <= LOG)
		appendStringInfo(&detail_buf, "\nStack:\n%s", detail->stack);

	ereport(elevel,
			(errcode(dl_error_sqlstate(code)),
			 errmsg("iceberg: %s failed", prefix),
			 errdetail("%s", detail_buf.data)));

	pfree(detail_buf.data);
}
