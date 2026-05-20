/*-------------------------------------------------------------------------
 *
 * queryenvironment.c
 *	  Query environment, to store context-specific values like ephemeral named
 *	  relations.  Initial use is for named tuplestores for delta information
 *	  from "normal" relations.
 *
 * The initial implementation uses a list because the number of such relations
 * in any one context is expected to be very small.  If that becomes a
 * performance problem, the implementation can be changed with no other impact
 * on callers, since this is an opaque structure.  This is the reason to
 * require a create function.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/queryenvironment.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "utils/queryenvironment.h"
#include "utils/rel.h"
#include "commands/matview.h"

/*
 * Private state of a query environment.
 */
struct QueryEnvironment
{
	List	*namedRelList;
	Size	snaplen;
	Oid 	matviewOid;
	Oid 	tableid;
	char	*snapname;
};

static List* construct_ENRList(QueryEnvironment *queryEnv);

QueryEnvironment *
create_queryEnv(void)
{
	return (QueryEnvironment *) palloc0(sizeof(QueryEnvironment));
}

/*
 * Configure the query environment with the given parameters.
 */
void
configure_queryEnv(QueryEnvironment *queryEnv, Oid matviewOid, Oid tableid, char* snapname, Size snaplen)
{
	if (queryEnv == NULL)
		return;
	queryEnv->matviewOid = matviewOid;
	queryEnv->tableid = tableid;
	queryEnv->snaplen = snaplen;
	queryEnv->snapname = snapname;
}


/*
 * Construct a list of EphemeralNamedRelationInfo objects from the namedRelList
 * in the given query environment.
 *
 * Parameters:
 *     - queryEnv: the query environment
 *
 * Returns:
 *     - a list of EphemeralNamedRelationInfo objects
 */
static List*
construct_ENRList(QueryEnvironment *queryEnv)
{
	ListCell   *lc;
	List	*enrList = NIL;
	Assert(queryEnv);

	foreach(lc, queryEnv->namedRelList)
	{
		EphemeralNamedRelation enr = (EphemeralNamedRelation) lfirst(lc);
		EphemeralNamedRelationInfo *node = palloc0(sizeof(EphemeralNamedRelationInfo));
		node->type = T_EphemeralNamedRelationInfo;
		node->natts = enr->md.tupdesc->natts;
		node->name = enr->md.name;
		node->reliddesc = enr->md.reliddesc;
		node->tuple = CreateTupleDescCopy(enr->md.tupdesc);
		node->enrtype = enr->md.enrtype;
		node->enrtuples = enr->md.enrtuples;
		enrList = lappend(enrList, node);
	}

	return enrList;
}

/*
 * Add preassigned EphemeralNamedRelationInfo objects to the given query environment.
 *
 * Parameters:
 *     - queryEnv: the query environment
 *     - enrs: a list of EphemeralNamedRelationInfo objects to add
 */
void
AddPreassignedENR(QueryEnvironment *queryEnv, List* enrs)
{
	ListCell   *lc;

	foreach(lc, enrs)
	{
		EphemeralNamedRelationInfo *enrinfo = (EphemeralNamedRelationInfo *) lfirst(lc);

		EphemeralNamedRelation enr = (EphemeralNamedRelation) palloc0(sizeof(EphemeralNamedRelationData));
		enr->md.name = enrinfo->name;
		enr->md.reliddesc = enrinfo->reliddesc;
		enr->md.tupdesc = enrinfo->tuple;
		enr->md.enrtype = enrinfo->enrtype;
		enr->md.enrtuples = enrinfo->enrtuples;
		enr->reldata = NULL;
		if (get_visible_ENR_metadata(queryEnv, enr->md.name) == NULL)
			register_ENR(queryEnv, enr);
		else
			pfree(enr);
	}
	return;
}

/*
 * Fill the QueryDispatchDesc structure with information from the given query environment.
 *
 * Parameters:
 *     - queryEnv: the query environment
 *     - ddesc: the QueryDispatchDesc structure to fill
 */
void
FillQueryDispatchDesc(QueryEnvironment *queryEnv, QueryDispatchDesc *ddesc)
{
	if (queryEnv == NULL)
		return;
	ddesc->namedRelList = construct_ENRList(queryEnv);
	ddesc->snaplen = queryEnv->snaplen;
	if (queryEnv->snaplen > 0)
	{
		ddesc->snapname = queryEnv->snapname;
		ddesc->matviewOid = queryEnv->matviewOid;
		ddesc->tableid = queryEnv->tableid;
	}
}

EphemeralNamedRelationMetadata
get_visible_ENR_metadata(QueryEnvironment *queryEnv, const char *refname)
{
	EphemeralNamedRelation enr;

	Assert(refname != NULL);

	if (queryEnv == NULL)
		return NULL;

	enr = get_ENR(queryEnv, refname);

	if (enr)
		return &(enr->md);

	return NULL;
}

/*
 * Register a named relation for use in the given environment.
 *
 * If this is intended exclusively for planning purposes, the tstate field can
 * be left NULL;
 */
void
register_ENR(QueryEnvironment *queryEnv, EphemeralNamedRelation enr)
{
	Assert(enr != NULL);
	Assert(get_ENR(queryEnv, enr->md.name) == NULL);

	queryEnv->namedRelList = lappend(queryEnv->namedRelList, enr);
}

/*
 * Unregister an ephemeral relation by name.  This will probably be a rarely
 * used function, but seems like it should be provided "just in case".
 */
void
unregister_ENR(QueryEnvironment *queryEnv, const char *name)
{
	EphemeralNamedRelation match;

	match = get_ENR(queryEnv, name);
	if (match)
		queryEnv->namedRelList = list_delete(queryEnv->namedRelList, match);
}

/*
 * This returns an ENR if there is a name match in the given collection.  It
 * must quietly return NULL if no match is found.
 */
EphemeralNamedRelation
get_ENR(QueryEnvironment *queryEnv, const char *name)
{
	ListCell   *lc;

	Assert(name != NULL);

	if (queryEnv == NULL)
		return NULL;

	foreach(lc, queryEnv->namedRelList)
	{
		EphemeralNamedRelation enr = (EphemeralNamedRelation) lfirst(lc);

		if (strcmp(enr->md.name, name) == 0)
			return enr;
	}

	return NULL;
}

/*
 * Gets the TupleDesc for a Ephemeral Named Relation, based on which field was
 * filled.
 *
 * When the TupleDesc is based on a relation from the catalogs, we count on
 * that relation being used at the same time, so that appropriate locks will
 * already be held.  Locking here would be too late anyway.
 */
TupleDesc
ENRMetadataGetTupDesc(EphemeralNamedRelationMetadata enrmd)
{
	TupleDesc	tupdesc;

	/* not only one, but use reliddesc field first. */
	Assert(enrmd->reliddesc || enrmd->tupdesc);

	if (enrmd->tupdesc != NULL)
		tupdesc = enrmd->tupdesc;
	else
	{
		Relation	relation;

		relation = table_open(enrmd->reliddesc, NoLock);
		tupdesc = relation->rd_att;
		table_close(relation, NoLock);
	}

	return tupdesc;
}
