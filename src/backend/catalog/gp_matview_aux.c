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
 * gp_matview_aux.c
 *
 *
 * IDENTIFICATION
 *	    src/backend/catalog/gp_matview_aux.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/genam.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/gp_matview_aux.h"
#include "catalog/gp_matview_tables.h"
#include "catalog/partition.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_type.h"
#include "catalog/indexing.h"
#include "cdb/cdbvars.h"
#include "commands/matview.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "storage/lockdefs.h"
#include "optimizer/optimizer.h"
#include "optimizer/transform.h"
#include "parser/parsetree.h"
#include "storage/lmgr.h"

static void InsertMatviewTablesEntries(Oid mvoid, List *relids);

static void RemoveMatviewTablesEntries(Oid mvoid);

static void SetMatviewAuxStatus_guts(Oid mvoid, char status);

static void addRelationMVRefCount(Oid relid, int32 mvrefcount);

/*
 * GetViewBaseRelids
 * Get all base tables's oid of a query tree.
 * Currently there is only one base table, but there should be
 * distinct func on it later. Self join tables: t1 join t1, will
 * get only one oid.
 * 
 * Return NIL if the query we think it's useless.
 */
List*
GetViewBaseRelids(const Query *viewQuery, bool *has_foreign)
{
	List	*relids = NIL;
	Node	*mvjtnode;

	if ((viewQuery->commandType != CMD_SELECT) ||
		(viewQuery->rowMarks != NIL) ||
		(viewQuery->distinctClause != NIL) ||
		(viewQuery->scatterClause != NIL) ||
		(viewQuery->cteList != NIL) ||
		(viewQuery->groupingSets != NIL) ||
		(viewQuery->havingQual != NULL) ||
		(viewQuery->setOperations != NULL) ||
		viewQuery->hasWindowFuncs ||
		viewQuery->hasDistinctOn ||
		viewQuery->hasModifyingCTE ||
		viewQuery->groupDistinct ||
		(viewQuery->parentStmtType == PARENTSTMTTYPE_REFRESH_MATVIEW) ||
		viewQuery->hasSubLinks)
	{
		return NIL;
	}

	if (tlist_has_srf(viewQuery))
	{
		return NIL;
	}

	/* As we will use views, make it strict to unmutable. */
	if (contain_mutable_functions((Node*)viewQuery))
		return NIL;

	if (list_length(viewQuery->jointree->fromlist) != 1)
		return NIL;

	mvjtnode = (Node *) linitial(viewQuery->jointree->fromlist);
	if (!IsA(mvjtnode, RangeTblRef))
		return NIL;

	RangeTblEntry	*rte = rt_fetch(1, viewQuery->rtable);
	if (rte->rtekind != RTE_RELATION)
		return NIL;

	char relkind = get_rel_relkind(rte->relid);

	/*
	 * Allow foreign table here, however we don't know if the data is
	 * up to date or not of the view.
	 * But if users want to query matview instead of query foreign tables
	 * outside CBDB, let them decide with aqumv_allow_foreign_table.
	 */
	if (relkind != RELKIND_RELATION &&
		relkind != RELKIND_PARTITIONED_TABLE &&
		relkind != RELKIND_FOREIGN_TABLE)
		return NIL;

	if (has_foreign)
		*has_foreign = relkind == RELKIND_FOREIGN_TABLE;

	/*
	 * inherit tables are not supported.
	 */
	bool can_be_partition = (relkind == RELKIND_PARTITIONED_TABLE) || get_rel_relispartition(rte->relid);

	if (!can_be_partition &&
		(has_superclass(rte->relid) || has_subclass(rte->relid)))
		return NIL;

	relids = list_make1_oid(rte->relid);

	return relids;
}

static void
add_view_dependency(Oid mvoid)
{
	ObjectAddress myself, referenced;

	ObjectAddressSet(myself, GpMatviewAuxId, mvoid);
	ObjectAddressSet(referenced, RelationRelationId, mvoid);
	recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
}

/*
 * InsertMatviewAuxEntry
 *  We also insert gp_matview_tables entry here to maintain view.
 */
void
InsertMatviewAuxEntry(Oid mvoid, const Query *viewQuery, bool skipdata)
{
	Relation	mvauxRel;
	HeapTuple	tup;
	bool		nulls[Natts_gp_matview_aux];
	Datum		values[Natts_gp_matview_aux];
	List 		*relids;
	NameData	mvname;
	bool		has_foreign = false;
	Relation		matviewRel;
	char	   *viewsql;

	Assert(OidIsValid(mvoid));

	/* Empty relids means the view is not supported now. */
	relids = GetViewBaseRelids(viewQuery, &has_foreign);
	if (relids == NIL)
		return;
	
	mvauxRel = table_open(GpMatviewAuxId, RowExclusiveLock);

	MemSet(nulls, false, sizeof(nulls));
	MemSet(values, 0, sizeof(values));

	values[Anum_gp_matview_aux_mvoid - 1] = ObjectIdGetDatum(mvoid);

	namestrcpy(&mvname, get_rel_name(mvoid));
	values[Anum_gp_matview_aux_mvname - 1] = NameGetDatum(&mvname);

	values[Anum_gp_matview_aux_has_foreign - 1] = BoolGetDatum(has_foreign);

	matviewRel = table_open(mvoid, NoLock);
	viewsql = nodeToString((Node *) copyObject(get_matview_query(matviewRel)));
	table_close(matviewRel, NoLock);
	values[Anum_gp_matview_aux_view_query - 1] = CStringGetTextDatum(viewsql);

	if (skipdata)
		values[Anum_gp_matview_aux_datastatus - 1] = CharGetDatum(MV_DATA_STATUS_EXPIRED);
	else
		values[Anum_gp_matview_aux_datastatus - 1] = CharGetDatum(MV_DATA_STATUS_UP_TO_DATE);

	tup = heap_form_tuple(RelationGetDescr(mvauxRel), values, nulls);

	CatalogTupleInsert(mvauxRel, tup);
	
	add_view_dependency(mvoid);

	/* Install view tables entries.*/
	InsertMatviewTablesEntries(mvoid, relids);

	table_close(mvauxRel, RowExclusiveLock);

	CommandCounterIncrement();

	return;
}

/*
 * Insert all relid oids for a materialized view.
 */
static void
InsertMatviewTablesEntries(Oid mvoid, List *relids)
{
	Relation	mtRel;
	HeapTuple	tup;
	bool		nulls[Natts_gp_matview_tables];
	Datum		values[Natts_gp_matview_tables];
	ListCell	*lc;

	mtRel = table_open(GpMatviewTablesId, RowExclusiveLock);
	MemSet(nulls, false, sizeof(nulls));
	MemSet(values, 0, sizeof(values));

	foreach(lc, relids)
	{
		Oid relid = lfirst_oid(lc);
		values[Anum_gp_matview_tables_relid - 1] = ObjectIdGetDatum(relid);
		values[Anum_gp_matview_tables_mvoid - 1] = ObjectIdGetDatum(mvoid);
		tup = heap_form_tuple(RelationGetDescr(mtRel), values, nulls);
		CatalogTupleInsert(mtRel, tup);

		/* update relation's pg_class entry */
		addRelationMVRefCount(relid, 1);
	}

	table_close(mtRel, RowExclusiveLock);
}

/*
 * RemoveMatviewAuxEntryOid
 *	Not all materialized views have a corresponding aux entry.
 *  We will skip those are certainly no used for AQUMV or
 * 	their date is awlays not up to date(ex: has volatile functions).
 */
void
RemoveMatviewAuxEntry(Oid mvoid)
{
	Relation	mvauxRel;
	HeapTuple	tup;

	mvauxRel = table_open(GpMatviewAuxId, RowExclusiveLock);

	tup = SearchSysCache1(MVAUXOID, ObjectIdGetDatum(mvoid));

 	/* This clould happen, not all materialized views have an aux entry. */
	if (!HeapTupleIsValid(tup))
	{
		ReleaseSysCache(tup);
		table_close(mvauxRel, RowExclusiveLock);
		return;
	}

	CatalogTupleDelete(mvauxRel, &tup->t_self);

	ReleaseSysCache(tup);

	RemoveMatviewTablesEntries(mvoid);

	table_close(mvauxRel, RowExclusiveLock);

	CommandCounterIncrement();

	return;
}

static void
RemoveMatviewTablesEntries(Oid mvoid)
{
	Relation	mtRel;
	CatCList   *catlist;
	int			i;
	Oid			relid;

	mtRel = table_open(GpMatviewTablesId, RowExclusiveLock);

	catlist = SearchSysCacheList1(MVTABLESMVRELOID, ObjectIdGetDatum(mvoid));
	for (i = 0; i < catlist->n_members; i++)
	{
		HeapTuple	tuple = &catlist->members[i]->tuple;

		/* This shouldn't happen, in case for that. */
		if (!HeapTupleIsValid(tuple))
			continue;

		/* update relation's pg_class entry */
		relid = ((Form_gp_matview_tables) GETSTRUCT(tuple))->relid;
		addRelationMVRefCount(relid, -1);

		CatalogTupleDelete(mtRel, &tuple->t_self);
	}

	ReleaseSysCacheList(catlist);

	table_close(mtRel, RowExclusiveLock);

	return;
}

/*
 * Set all relative materialized views status if
 * the underlying table's data is changed.
 */
void
SetRelativeMatviewAuxStatus(Oid relid, char status, char direction)
{
	Relation	mvauxRel;
	Relation	mtRel;
	HeapTuple	tup;
	ScanKeyData key;
	SysScanDesc desc;
	List		*base_oids;
	ListCell   *cell;

	/*
	 * Do a quick check if relation has relative materialized views.
	 */
	if (get_rel_relmvrefcount(relid) <= 0)
		return;

	mvauxRel = table_open(GpMatviewAuxId, RowExclusiveLock);

	/* Find all mvoids have relid */
	mtRel = table_open(GpMatviewTablesId, AccessShareLock);

	/*
	 * For partitioned table, transfer status to children.
	 * For patition, transfer status to all ancestors.
	 */
	if (get_rel_relkind(relid) == RELKIND_PARTITIONED_TABLE &&
		(MV_DATA_STATUS_TRANSFER_DIRECTION_ALL == direction ||
		MV_DATA_STATUS_TRANSFER_DIRECTION_DOWN == direction))
		base_oids = find_all_inheritors(relid, NoLock, NULL);
	else
		base_oids = list_make1_oid(relid);

	if (get_rel_relispartition(relid) &&
		(MV_DATA_STATUS_TRANSFER_DIRECTION_ALL == direction ||
		MV_DATA_STATUS_TRANSFER_DIRECTION_UP == direction))
		base_oids = list_concat(base_oids, get_partition_ancestors(relid));

	foreach(cell, base_oids)
	{
		Oid base_oid = lfirst_oid(cell);
		ScanKeyInit(&key,
			Anum_gp_matview_tables_relid,
			BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(base_oid));
		desc = systable_beginscan(mtRel,
									  GpMatviewTablesRelIndexId,
									  true,
									  NULL, 1, &key);
		while (HeapTupleIsValid(tup = systable_getnext(desc)))
		{
			Form_gp_matview_tables mt = (Form_gp_matview_tables) GETSTRUCT(tup);
			/* Update mv aux status. */
			SetMatviewAuxStatus_guts(mt->mvoid, status);
		}

		systable_endscan(desc);
	}

	table_close(mtRel, AccessShareLock);
	table_close(mvauxRel, RowExclusiveLock);
}

void
SetMatviewAuxStatus(Oid mvoid, char status)
{
	Relation	mvauxRel;
	mvauxRel = table_open(GpMatviewAuxId, RowExclusiveLock);
	SetMatviewAuxStatus_guts(mvoid, status);
	table_close(mvauxRel, RowExclusiveLock);
}

/*
 * This requires caller has opened the table and
 * holds proper locks.
 */
static void
SetMatviewAuxStatus_guts(Oid mvoid, char status)
{
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Relation 	mvauxRel;	
	Datum		valuesAtt[Natts_gp_matview_aux];
	bool		nullsAtt[Natts_gp_matview_aux];
	bool		replacesAtt[Natts_gp_matview_aux];

	switch (status)
	{
		case MV_DATA_STATUS_UP_TO_DATE:
		case MV_DATA_STATUS_UP_REORGANIZED:
		case MV_DATA_STATUS_EXPIRED:
		case MV_DATA_STATUS_EXPIRED_INSERT_ONLY:
			break;
		default:
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("invalid status: %c", status)));
	}

	if (!IS_QUERY_DISPATCHER())
	{
		ereport(WARNING, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg("try to modify matview catalog outside QD")));
		return;
	}

	tuple = SearchSysCacheCopy1(MVAUXOID, ObjectIdGetDatum(mvoid));

	/* This could happen, Refresh a matview we didn't install in aux table. */
	if (!HeapTupleIsValid(tuple))
		return;

	Form_gp_matview_aux auxform = (Form_gp_matview_aux) GETSTRUCT(tuple);

	/* Nothing to do. */
	if (auxform->datastatus == status)
		return;

	/* handle incomming insert only stauts */
	if (status == MV_DATA_STATUS_EXPIRED_INSERT_ONLY)
	{
		switch (auxform->datastatus)
		{
			case MV_DATA_STATUS_EXPIRED:
				return; /* keep expired. */
			case MV_DATA_STATUS_UP_TO_DATE:
				break; /* just update. */
			case MV_DATA_STATUS_UP_REORGANIZED:
			{
				/*
				 * Insert comes after VACUUM FULL or CLUSTER.
				 * There are pages reorganized and more date inserted.
				 * Neighter we can use this view as logically up to date
				 * nor fetch  Delta pages on base table.
				 */
				status = MV_DATA_STATUS_EXPIRED;
				break;
			}
			default:
				Assert(false); /* should not get here. */
				break;
		}
	}

	/* handle incomming reorganized stauts */
	if (status == MV_DATA_STATUS_UP_REORGANIZED)
	{
		switch (auxform->datastatus)
		{
			case MV_DATA_STATUS_EXPIRED:
				return; /* keep expired. */
			case MV_DATA_STATUS_UP_TO_DATE:
				break; /* just update. */
			case MV_DATA_STATUS_EXPIRED_INSERT_ONLY:
			{
				/*
				 * Reorganize pages, Delta pages can not be
				 * fetched anymore.
				 */
				status = MV_DATA_STATUS_EXPIRED;
				break;
			}
			default:
				Assert(false); /* should not get here. */
				break;
		}
	}

	MemSet(valuesAtt, 0, sizeof(valuesAtt));
	MemSet(nullsAtt, false, sizeof(nullsAtt));
	MemSet(replacesAtt, false, sizeof(replacesAtt));

	replacesAtt[Anum_gp_matview_aux_datastatus -1] = true;
	valuesAtt[Anum_gp_matview_aux_datastatus - 1] = CharGetDatum(status);

	mvauxRel = table_open(GpMatviewAuxId, NoLock);

	newtuple = heap_modify_tuple(tuple, RelationGetDescr(mvauxRel),
									  valuesAtt, nullsAtt, replacesAtt);

	CatalogTupleUpdate(mvauxRel, &newtuple->t_self, newtuple);
	heap_freetuple(newtuple);
	table_close(mvauxRel, NoLock);

	/* For partitioned table, we may insert into same rel multiple times. */
	CommandCounterIncrement();
}

/*
 * For SERVERLESS:
 * Could view be used for Append Agg plan?
 * This is only used for Incremental Append Agg plan purpose.
 *
 * If staus is insert only, we could expand Append Agg plan
 * with the view.
 * If status is up to date, we still do that where SeqScan on
 * base table should get zero tuples.
 * But VACUUM FULL and CLUTER commands will change the pages of
 * base table, we could not fetch a Delta pages for Delta SeqScan.
 */
bool
MatviewUsableForAppendAgg(Oid mvoid)
{
	HeapTuple mvauxtup = SearchSysCacheCopy1(MVAUXOID, ObjectIdGetDatum(mvoid));
	if (!HeapTupleIsValid(mvauxtup))
		return false;

	Form_gp_matview_aux auxform = (Form_gp_matview_aux) GETSTRUCT(mvauxtup);
	return ((auxform->datastatus == MV_DATA_STATUS_UP_TO_DATE) || 
			(auxform->datastatus == MV_DATA_STATUS_EXPIRED_INSERT_ONLY));
}

bool
MatviewHasForeignTables(Oid mvoid)
{
	HeapTuple mvauxtup = SearchSysCacheCopy1(MVAUXOID, ObjectIdGetDatum(mvoid));

	/* Not a candidate we recorded. */
	if (!HeapTupleIsValid(mvauxtup))
		return false;

	Form_gp_matview_aux auxform = (Form_gp_matview_aux) GETSTRUCT(mvauxtup);
	return auxform->has_foreign;
}

/*
 * Is the view data up to date?
 * In most cases, we should use this function to check if view
 * data status is up to date.
 * 
 * We include the cases VACUUM FULL/CLUSTER on base tables.
 * Their data is logically not changed.
 */
bool
MatviewIsGeneralyUpToDate(Oid mvoid)
{
	HeapTuple mvauxtup = SearchSysCacheCopy1(MVAUXOID, ObjectIdGetDatum(mvoid));

	/* Not a candidate we recorded. */
	if (!HeapTupleIsValid(mvauxtup))
		return false;

	Form_gp_matview_aux auxform = (Form_gp_matview_aux) GETSTRUCT(mvauxtup);
	return ((auxform->datastatus == MV_DATA_STATUS_UP_TO_DATE) || 
			(auxform->datastatus == MV_DATA_STATUS_UP_REORGANIZED));
}

bool
MatviewIsUpToDate(Oid mvoid)
{
	HeapTuple mvauxtup = SearchSysCacheCopy1(MVAUXOID, ObjectIdGetDatum(mvoid));

	/* Not a candidate we recorded. */
	if (!HeapTupleIsValid(mvauxtup))
		return false;

	Form_gp_matview_aux auxform = (Form_gp_matview_aux) GETSTRUCT(mvauxtup);
	return (auxform->datastatus == MV_DATA_STATUS_UP_TO_DATE);
}

static void
addRelationMVRefCount(Oid relid, int32 mvrefcount)
{
	Relation	pgrel;
	HeapTuple	tuple;
	ItemPointerData otid;

	pgrel = table_open(RelationRelationId, RowExclusiveLock);
	/*
	 * Update relation's pg_class entry.
	 */
	tuple = SearchSysCacheLockedCopy1(RELOID,
									  ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	otid = tuple->t_self;
	((Form_pg_class) GETSTRUCT(tuple))->relmvrefcount += mvrefcount;

	CatalogTupleUpdate(pgrel, &otid, tuple);

	UnlockTuple(pgrel, &otid, InplaceUpdateTupleLock);

	table_close(pgrel, RowExclusiveLock);
}

/*
 * Rename matview's name in gp_matview_aux.
 */
void
mvaux_rename(Oid mvoid, char* newname)
{
	HeapTuple	tuple;
	HeapTuple	newtuple;
	Relation 	mvauxRel;
	NameData	mvname;
	Datum		valuesAtt[Natts_gp_matview_aux];
	bool		nullsAtt[Natts_gp_matview_aux];
	bool		replacesAtt[Natts_gp_matview_aux];

	tuple = SearchSysCacheCopy1(MVAUXOID, ObjectIdGetDatum(mvoid));

	if (!HeapTupleIsValid(tuple))
		return;

	mvauxRel = table_open(GpMatviewAuxId, RowExclusiveLock);

	MemSet(valuesAtt, 0, sizeof(valuesAtt));
	MemSet(nullsAtt, false, sizeof(nullsAtt));
	MemSet(replacesAtt, false, sizeof(replacesAtt));

	replacesAtt[Anum_gp_matview_aux_mvname -1] = true;

	namestrcpy(&mvname, newname);
	valuesAtt[Anum_gp_matview_aux_mvname - 1] = NameGetDatum(&mvname);

	newtuple = heap_modify_tuple(tuple, RelationGetDescr(mvauxRel),
									  valuesAtt, nullsAtt, replacesAtt);

	CatalogTupleUpdate(mvauxRel, &newtuple->t_self, newtuple);
	heap_freetuple(newtuple);
	table_close(mvauxRel, RowExclusiveLock);

	CommandCounterIncrement();
}
