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
 * lance_import.c
 *	  IMPORT FOREIGN SCHEMA for lance_fdw.
 *
 * LIMIT TO is mandatory: lance-c has no API that enumerates the datasets under
 * a prefix, and inventing one out of an S3 listing would be a second
 * dependency surface.  Saying so out loud is more honest than pretending the
 * wrapper has a catalog.
 *
 * B-tier columns are skipped with a NOTICE naming the column and its Arrow
 * type, and a dataset with nothing left to import is an error rather than
 * an empty table.
 *
 * A struct column needs a PostgreSQL composite type, and this is the only place
 * that knows enough to make one: the type is named after the foreign table and
 * the field path, so it cannot be made before the table is known.
 * The types are created here rather than returned as commands because the
 * server executes nothing but CreateForeignTableStmt out of an FDW's command
 * list; they are ordinary database objects afterwards, which is why dropping
 * the foreign table leaves them behind (README, Known Limits).
 *
 * IDENTIFICATION
 *	  src/lance_import.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lance_fdw.h"
#include "lance_arrow.h"
#include "lance_import.h"
#include "lance_option.h"
#include "lance_runtime.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

/*
 * How many names a composite type may be offered before the import gives up.
 * Only a name long enough to be truncated can need a second one: two field
 * paths that differ beyond the 63rd byte would otherwise silently share a type.
 */
#define LANCE_IMPORT_NAME_TRIES 100

/* What one import call needs to know to build composite types. */
typedef struct LanceImportState
{
	const char *schema;			/* the local schema, as the statement names it */
	Oid			nspoid;
} LanceImportState;

/* A PostgreSQL type, as a column definition needs it and as a catalog has it. */
typedef struct LanceImportType
{
	Oid			typid;
	int32		typmod;
	const char *sql;			/* how to spell it in a CREATE statement */
} LanceImportType;

static LanceImportType lance_import_field_type(LanceImportState *state,
											   const char *table,
											   const char *path,
											   const struct ArrowSchema *field);

/*
 * The composite type naming rule: lance_<table>_<field path>,
 * lower case, with everything outside [a-z0-9_] replaced.  Following pglance
 * here is deliberate - a user who has both wrappers should not have to learn
 * two conventions - and so is folding rather than dropping the odd characters:
 * dropping them would map "a.b" and "ab" to one name.
 */
static void
lance_import_append_sanitized(StringInfo buf, const char *ident)
{
	const char *c;

	for (c = ident; *c != '\0'; c++)
	{
		if (*c >= 'a' && *c <= 'z')
			appendStringInfoChar(buf, *c);
		else if (*c >= 'A' && *c <= 'Z')
			appendStringInfoChar(buf, (char) (*c - 'A' + 'a'));
		else if ((*c >= '0' && *c <= '9') || *c == '_')
			appendStringInfoChar(buf, *c);
		else
			appendStringInfoChar(buf, '_');
	}
}

static char *
lance_import_type_base(const char *table, const char *path)
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfoString(&buf, "lance_");
	lance_import_append_sanitized(&buf, table);
	appendStringInfoChar(&buf, '_');
	lance_import_append_sanitized(&buf, path);

	return buf.data;
}

/*
 * The seq'th name to offer for a composite type: the base, cut to what an
 * identifier holds, with a number appended from the second try on.  The cut is
 * by bytes, which is exact because the base has been folded to [a-z0-9_].
 */
static char *
lance_import_type_candidate(const char *base, int seq)
{
	StringInfoData buf;
	char		suffix[16];
	int			limit = NAMEDATALEN - 1;

	suffix[0] = '\0';
	if (seq > 0)
		snprintf(suffix, sizeof(suffix), "_%d", seq);

	limit -= (int) strlen(suffix);

	initStringInfo(&buf);
	appendStringInfoString(&buf, base);
	if (buf.len > limit)
	{
		buf.len = limit;
		buf.data[limit] = '\0';
	}
	appendStringInfoString(&buf, suffix);

	return buf.data;
}

static Oid
lance_import_find_type(Oid nspoid, const char *name)
{
	return GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
						   PointerGetDatum(name),
						   ObjectIdGetDatum(nspoid));
}

/*
 * Is the type that already carries this name the one this import would create?
 *
 * Field for field, by name and by type: a composite type whose second field is
 * text where this dataset has an integer is a different type that happens to
 * share a name, and reusing it would read the column wrongly.  A field whose
 * type this import has not created yet has no OID and therefore never matches,
 * which is right - a type that existed before cannot reference one that did not.
 */
static bool
lance_import_type_matches(Oid typoid, char **names, LanceImportType *types,
						  int n)
{
	TupleDesc	tupdesc;
	bool		ok = true;
	int			live = 0;
	int			att = 0;
	int			i;

	if (get_typtype(typoid) != TYPTYPE_COMPOSITE)
		return false;

	tupdesc = lookup_rowtype_tupdesc_copy(typoid, -1);

	for (i = 0; i < tupdesc->natts; i++)
	{
		if (!TupleDescAttr(tupdesc, i)->attisdropped)
			live++;
	}

	if (live != n)
		ok = false;

	for (i = 0; ok && i < n; i++)
	{
		Form_pg_attribute attr;

		while (att < tupdesc->natts && TupleDescAttr(tupdesc, att)->attisdropped)
			att++;

		attr = TupleDescAttr(tupdesc, att);
		att++;

		if (strcmp(NameStr(attr->attname), names[i]) != 0 ||
			attr->atttypid != types[i].typid ||
			attr->atttypmod != types[i].typmod)
			ok = false;
	}

	FreeTupleDesc(tupdesc);

	return ok;
}

/*
 * Create one composite type, now rather than through the returned command list.
 *
 * SPI runs it through the ordinary utility path, which is what makes the type
 * exist on the segments too and with the same OID.  An error anywhere later in
 * the import aborts the transaction and takes the type with it.
 */
static void
lance_import_create_type(LanceImportState *state, const char *name,
						 char **names, LanceImportType *types, int n)
{
	StringInfoData buf;
	int			i;
	int			rc;

	initStringInfo(&buf);
	appendStringInfo(&buf, "CREATE TYPE %s.%s AS (",
					 quote_identifier(state->schema), quote_identifier(name));

	for (i = 0; i < n; i++)
		appendStringInfo(&buf, "%s%s %s", i > 0 ? ", " : "",
						 quote_identifier(names[i]), types[i].sql);

	appendStringInfoChar(&buf, ')');

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("lance_fdw: could not connect to SPI to create composite type \"%s\"",
						name)));

	rc = SPI_execute(buf.data, false, 0);
	if (rc != SPI_OK_UTILITY)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("lance_fdw: could not create composite type \"%s\"",
						name),
				 errdetail("SPI_execute returned %d.", rc)));

	SPI_finish();

	/* The next lookup, including this type's own parent, has to see it. */
	CommandCounterIncrement();
}

/*
 * The composite type for one Arrow struct, created or reused.
 *
 * Subfields are resolved first, so a struct of a struct has the inner type by
 * the time the outer one is declared, and so that the comparison against an
 * existing type can be made on OIDs.
 *
 * A name that is already taken is reused when the definition is the same and is
 * an error when it is not; a name long enough to have been cut down
 * gets a numbered one instead, because there the collision is this rule's doing
 * rather than the user's.
 */
static LanceImportType
lance_import_composite(LanceImportState *state, const char *table,
					   const char *path, const struct ArrowSchema *field,
					   bool as_array)
{
	int			n = (int) field->n_children;
	char	  **names = (char **) palloc(sizeof(char *) * Max(n, 1));
	LanceImportType *types = (LanceImportType *)
		palloc(sizeof(LanceImportType) * Max(n, 1));
	LanceImportType out;
	char	   *base;
	char	   *name = NULL;
	Oid			typoid = InvalidOid;
	bool		truncated;
	int			seq;
	int			i;

	for (i = 0; i < n; i++)
	{
		const struct ArrowSchema *child = field->children[i];

		if (child == NULL || child->name == NULL || child->name[0] == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_COLUMN_NAME),
					 errmsg("lance_fdw: subfield %d of \"%s\" has no name",
							i + 1, path)));

		names[i] = pstrdup(child->name);
		types[i] = lance_import_field_type(state, table,
										   psprintf("%s_%s", path, names[i]),
										   child);
	}

	base = lance_import_type_base(table, path);
	truncated = strlen(base) > NAMEDATALEN - 1;

	for (seq = 0; seq < LANCE_IMPORT_NAME_TRIES; seq++)
	{
		name = lance_import_type_candidate(base, seq);
		typoid = lance_import_find_type(state->nspoid, name);

		if (!OidIsValid(typoid))
		{
			lance_import_create_type(state, name, names, types, n);
			typoid = lance_import_find_type(state->nspoid, name);
			break;
		}

		if (lance_import_type_matches(typoid, names, types, n))
			break;				/* the same type under the same name: reuse it */

		if (!truncated)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("lance_fdw: type \"%s.%s\" already exists and is not the composite type \"%s\" needs",
							state->schema, name, path),
					 errdetail("A struct column is read as the composite type named after the foreign table and the field path."),
					 errhint("Drop the type, or import into a schema that does not have it.")));
	}

	if (!OidIsValid(typoid))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("lance_fdw: could not find a free name for the composite type of \"%s\"",
						path),
				 errdetail("The name is cut to %d bytes and all %d numbered forms of \"%s\" are taken.",
						   NAMEDATALEN - 1, LANCE_IMPORT_NAME_TRIES, base)));

	out.typid = as_array ? get_array_type(typoid) : typoid;
	out.typmod = -1;
	out.sql = psprintf("%s.%s%s", quote_identifier(state->schema),
					   quote_identifier(name), as_array ? "[]" : "");

	return out;
}

/*
 * The PostgreSQL type for one A-tier Arrow field, as a column definition needs
 * to spell it.  Only the struct shapes need anything beyond the type map: a
 * struct becomes its own composite type, and a list of structs an array of one.
 */
static LanceImportType
lance_import_field_type(LanceImportState *state, const char *table,
						const char *path, const struct ArrowSchema *field)
{
	LanceImportType out;
	Oid			typid;
	int32		typmod;
	bool		is_b_tier;

	/* The caller has already established that the field is A-tier. */
	if (!lance_arrow_map_type(field, &typid, &typmod, &is_b_tier))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				 errmsg("lance_fdw: \"%s\" has Arrow type \"%s\", which is not supported",
						path, lance_arrow_format(field))));

	if (typid == RECORDOID)
		return lance_import_composite(state, table, path, field, false);

	if (typid == RECORDARRAYOID)
		return lance_import_composite(state, table, path,
									  field->children[0], true);

	out.typid = typid;
	out.typmod = typmod;
	out.sql = format_type_with_typemod(typid, typmod);

	return out;
}

static void
lance_import_columns(StringInfo buf, ImportForeignSchemaStmt *stmt,
					 ForeignServer *server, const char *name,
					 struct ArrowSchema *schema)
{
	LanceImportState state;
	int			ncols = 0;
	int			nskipped = 0;
	int64		i;

	state.schema = stmt->local_schema;
	state.nspoid = get_namespace_oid(stmt->local_schema, false);

	appendStringInfo(buf, "CREATE FOREIGN TABLE %s.%s (\n",
					 quote_identifier(stmt->local_schema),
					 quote_identifier(name));

	for (i = 0; i < schema->n_children; i++)
	{
		struct ArrowSchema *field = schema->children[i];
		const char *colname;
		LanceImportType coltype;
		Oid			typid;
		int32		typmod;
		bool		is_b_tier;

		if (field == NULL || field->name == NULL || field->name[0] == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_COLUMN_NAME),
					 errmsg("lance_fdw: field %d of dataset \"%s\" has no name",
							(int) i, name)));

		colname = field->name;

		if (!lance_arrow_map_type(field, &typid, &typmod, &is_b_tier))
		{
			if (lance_arrow_is_blob_encoded(field))
				ereport(NOTICE,
						(errmsg("skipping column \"%s\" of \"%s\": Lance blob encoding is not supported",
								colname, name)));
			else
			{
				const char *typname = lance_arrow_type_name(field);

				/*
				 * Where the reason is inside the field - a subfield, a map key,
				 * a Blob v2 descriptor - the type alone does not say why, so the
				 * detail names the part that is at fault.
				 */
				char	   *detail = lance_arrow_b_tier_detail(field);

				ereport(NOTICE,
						(errmsg("skipping column \"%s\" of \"%s\": Arrow type \"%s\" (%s) is not supported",
								colname, name, lance_arrow_format(field),
								typname != NULL ? typname : "unparsable"),
						 detail != NULL ? errdetail("%s", detail) : 0));
			}
			nskipped++;
			continue;
		}

		coltype = lance_import_field_type(&state, name, colname, field);

		if (ncols > 0)
			appendStringInfoString(buf, ",\n");

		appendStringInfo(buf, "  %s %s",
						 quote_identifier(colname), coltype.sql);

		/*
		 * A name PostgreSQL would truncate has to be pinned down with a
		 * column_name option, or the scan would look for the truncated name.
		 */
		if (strlen(colname) >= NAMEDATALEN)
			appendStringInfo(buf, " OPTIONS (column_name %s)",
							 quote_literal_cstr(colname));

		ncols++;
	}

	if (ncols == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("dataset \"%s\" has no column lance_fdw can read", name),
				 errdetail("All %d columns have unsupported types.", nskipped)));

	appendStringInfo(buf, "\n) SERVER %s\n  OPTIONS (uri %s)",
					 quote_identifier(server->servername),
					 quote_literal_cstr(name));
}

/*
 * Open one dataset and turn its schema into a CREATE FOREIGN TABLE statement.
 * The dataset handle and the exported ArrowSchema are both released on the
 * error path as well as the normal one.
 */
static char *
lance_import_one(ImportForeignSchemaStmt *stmt, ForeignServer *server,
				 const char *base_uri, const char **storage_opts,
				 const char *name)
{
	char	   *uri = lance_resolve_uri(base_uri, name);
	LanceDataset *dataset;
	LanceHandle *handle;
	LanceSession *session;
	struct ArrowSchema schema;
	StringInfoData buf;
	int32		rc;

	/* Brings the runtime up, and may ereport: outside the masked call. */
	session = lance_rt_session();

	LANCE_MASKED(dataset = lance_dataset_open_with_session(uri,
														   (const char *const *) storage_opts,
														   0,
														   session));
	LANCE_CHECK(dataset != NULL, uri);
	handle = lance_rt_track_dataset(dataset);

	memset(&schema, 0, sizeof(schema));
	LANCE_MASKED(rc = lance_dataset_schema(dataset, &schema));
	LANCE_CHECK(rc == 0, uri);

	initStringInfo(&buf);

	PG_TRY();
	{
		lance_import_columns(&buf, stmt, server, name, &schema);
	}
	PG_FINALLY();
	{
		if (schema.release != NULL)
			LANCE_MASKED(schema.release(&schema));
	}
	PG_END_TRY();

	lance_rt_release(handle);

	return buf.data;
}

List *
lanceImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
	ForeignServer *server;
	char	   *base_uri;
	const char **storage_opts;
	List	   *commands = NIL;
	ListCell   *lc;

	if (stmt->list_type != FDW_IMPORT_SCHEMA_LIMIT_TO)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("IMPORT FOREIGN SCHEMA for lance_fdw requires LIMIT TO"),
				 errdetail("Lance has no catalog that lists the datasets under a uri."),
				 errhint("Name the datasets, for example LIMIT TO (\"sales.lance\").")));

	server = GetForeignServer(serverOid);
	base_uri = lance_server_base_uri(serverOid);
	storage_opts = lance_build_storage_options(serverOid, GetUserId());

	lance_rt_init();

	foreach(lc, stmt->table_list)
	{
		RangeVar   *rv = (RangeVar *) lfirst(lc);

		commands = lappend(commands,
						   lance_import_one(stmt, server, base_uri,
											storage_opts, rv->relname));
	}

	return commands;
}
