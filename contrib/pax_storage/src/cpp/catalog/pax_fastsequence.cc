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
 * pax_fastsequence.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/catalog/pax_fastsequence.cc
 *
 *-------------------------------------------------------------------------
 */

#include "catalog/pax_fastsequence.h"

#include "comm/cbdb_api.h"

#include "comm/cbdb_wrappers.h"

// Get the required objid Tuple from pg_pax_fastsequence system table.
// objid indicates single pax micro-partition table oid.
// lock_mode indicates the lock level used when retrive data from system table.
static HeapTuple CPaxOpenFastSequenceTable(Oid objid,
                                           Relation *pax_fastsequence_rel,
                                           SysScanDesc *pax_fastsequece_scan,
                                           LOCKMODE lock_mode) {
  ScanKeyData scankey[1];
  HeapTuple tuple;
  Relation rel;
  SysScanDesc scan;

  rel = table_open(PAX_FASTSEQUENCE_OID, lock_mode);

  /* SELECT * FROM paxaux.pg_pax_fastsequence WHERE objid = :1 FOR UPDATE */
  ScanKeyInit(&scankey[0], ANUM_PG_PAX_FAST_SEQUENCE_OBJID,
              BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objid));

  scan = systable_beginscan(rel, PAX_FASTSEQUENCE_INDEX_OID, true, NULL, 1,
                            scankey);
  tuple = systable_getnext(scan);
  if (HeapTupleIsValid(tuple)) {
    // lock the CTID
    LockTuple(rel, &(tuple->t_self), ExclusiveLock);
  }

  *pax_fastsequence_rel = rel;
  *pax_fastsequece_scan = scan;

  return tuple;
}

static inline void CPaxCloseFastSequenceTable(Relation pax_fastsequence_rel,
                                              SysScanDesc pax_fastsequece_scan,
                                              HeapTuple tuple,
                                              LOCKMODE lock_mode) {
  if (HeapTupleIsValid(tuple)) {
    UnlockTuple(pax_fastsequence_rel, &(tuple->t_self), ExclusiveLock);
  }

  systable_endscan(pax_fastsequece_scan);
  table_close(pax_fastsequence_rel, lock_mode);
}

// update the existing fast sequence number for (objid).
// This tuple is updated with the new value. Otherwise, a new tuple is inserted
// into the table.
static void CPaxUpdateFastsequence(Relation pax_fastsequence_rel,
                                   HeapTuple old_tuple, TupleDesc tuple_desc,
                                   Oid objid, int32 new_seqno) {
  HeapTuple new_tuple;
  Datum values[NATTS_PG_PAX_FAST_SEQUENCE_TABLES];
  bool nulls[NATTS_PG_PAX_FAST_SEQUENCE_TABLES];

  // If such a tuple does not exist, insert a new one.
  Assert(HeapTupleIsValid(old_tuple));

  values[ANUM_PG_PAX_FAST_SEQUENCE_OBJID - 1] = ObjectIdGetDatum(objid);
  values[ANUM_PG_PAX_FAST_SEQUENCE_LASTSEQUENCE - 1] = Int32GetDatum(new_seqno);
  nulls[ANUM_PG_PAX_FAST_SEQUENCE_OBJID - 1] = false;
  nulls[ANUM_PG_PAX_FAST_SEQUENCE_LASTSEQUENCE - 1] = false;

  new_tuple = heap_form_tuple(tuple_desc, values, nulls);
  Assert(HeapTupleIsValid(new_tuple));

  new_tuple->t_data->t_ctid = old_tuple->t_data->t_ctid;
  new_tuple->t_self = old_tuple->t_self;

  heap_inplace_update(pax_fastsequence_rel, new_tuple);
  heap_freetuple(new_tuple);
}

// InitializeFastSequenceEntry is used to generate and keep track of allocated
// micropartition file number. objid indicates single pax micro-partition table
// oid. lastsequence indicates the current allocated file number by using
// fastsequence allocation.
void CPaxInitializeFastSequenceEntry(Oid objid, char init_type,
                                     int32 fast_seq) {
  Relation pax_fastsequence_rel;
  SysScanDesc scan;
  TupleDesc desc;
  HeapTuple tuple;
  HeapTuple new_tuple;
  Datum values[NATTS_PG_PAX_FAST_SEQUENCE_TABLES];
  bool nulls[NATTS_PG_PAX_FAST_SEQUENCE_TABLES];

  Assert(init_type == FASTSEQUENCE_INIT_TYPE_CREATE ||
         init_type == FASTSEQUENCE_INIT_TYPE_INPLACE ||
         init_type == FASTSEQUENCE_INIT_TYPE_UPDATE);
  // Initilize a new object id and use row-based exclusive lock to avoid
  // concurrency issue.
  tuple = CPaxOpenFastSequenceTable(objid, &pax_fastsequence_rel, &scan,
                                    RowExclusiveLock);
  AssertImply(init_type == FASTSEQUENCE_INIT_TYPE_CREATE, fast_seq == 0);
  desc = RelationGetDescr(pax_fastsequence_rel);
  values[ANUM_PG_PAX_FAST_SEQUENCE_OBJID - 1] = ObjectIdGetDatum(objid);
  values[ANUM_PG_PAX_FAST_SEQUENCE_LASTSEQUENCE - 1] = Int32GetDatum(fast_seq);
  nulls[ANUM_PG_PAX_FAST_SEQUENCE_OBJID - 1] = false;
  nulls[ANUM_PG_PAX_FAST_SEQUENCE_LASTSEQUENCE - 1] = false;
  new_tuple = heap_form_tuple(desc, values, nulls);

  if (init_type == FASTSEQUENCE_INIT_TYPE_CREATE) {
    ObjectAddress base;
    ObjectAddress aux;

    if (HeapTupleIsValid(tuple))
      elog(ERROR,
           "existing tuple in pg_pax_fastsequence when creating pax table");

    CatalogTupleInsert(pax_fastsequence_rel, new_tuple);

    base.classId = RelationRelationId;
    base.objectId = objid;
    base.objectSubId = 0;
    aux.classId = PAX_FASTSEQUENCE_OID;
    aux.objectId = objid;
    aux.objectSubId = 0;
    recordDependencyOn(&aux, &base, DEPENDENCY_INTERNAL);
  } else {
    // exists, set to 0 in-place, or update
    if (!HeapTupleIsValid(tuple))
      elog(ERROR,
           "no tuple found in pg_pax_fastsequence for existing pax table");

    new_tuple->t_data->t_ctid = tuple->t_data->t_ctid;
    new_tuple->t_self = tuple->t_self;
    if (init_type == FASTSEQUENCE_INIT_TYPE_INPLACE)
      heap_inplace_update(pax_fastsequence_rel, new_tuple);
    else if (init_type == FASTSEQUENCE_INIT_TYPE_UPDATE)
      CatalogTupleUpdate(pax_fastsequence_rel, &new_tuple->t_self, new_tuple);
  }

  heap_freetuple(new_tuple);
  CPaxCloseFastSequenceTable(pax_fastsequence_rel, scan, tuple, 
                             RowExclusiveLock);
}

// GetFastSequences
// Get consecutive sequence numbers, the returned sequence number is the
// lastsequence + 1
int32 CPaxGetFastSequences(Oid objid, bool increase) {
  Relation pax_fastsequence_rel = NULL;
  SysScanDesc scan = NULL;
  TupleDesc tuple_desc;
  HeapTuple tuple;
  Datum seqno_datum;
  int32 seqno;
  bool isnull = false;

  // Increase and read sequence number base on objid and use row-based exclusive
  // lock to avoid concurrency issue.
  tuple = CPaxOpenFastSequenceTable(objid, &pax_fastsequence_rel, &scan,
                                    RowExclusiveLock);

  Assert(HeapTupleIsValid(tuple));

  tuple_desc = RelationGetDescr(pax_fastsequence_rel);

  seqno_datum = heap_getattr(tuple, ANUM_PG_PAX_FAST_SEQUENCE_LASTSEQUENCE,
                             tuple_desc, &isnull);
  if (isnull) {
    ereport(
        ERROR,
        (errcode(ERRCODE_UNDEFINED_OBJECT),
         errmsg(
             "CPaxGetFastSequences got an invalid lastsequence number: NULL")));
  }
  seqno = DatumGetInt32(seqno_datum);
  if (seqno < 0) elog(ERROR, "sequence number out of range: %d", seqno);

  if (increase) {
    CPaxUpdateFastsequence(pax_fastsequence_rel, tuple, tuple_desc, objid,
                           seqno + 1);
  }

  CPaxCloseFastSequenceTable(pax_fastsequence_rel, scan, tuple, 
                             RowExclusiveLock);

  return seqno;
}

char *CPaxGetFastSequencesName(Oid oid, bool missing_ok) {
  char *pax_fs_name;
  Relation pax_fastsequence_rel = NULL;
  SysScanDesc scan = NULL;
  HeapTuple tuple;

  tuple = CPaxOpenFastSequenceTable(oid, &pax_fastsequence_rel, &scan,
                                    RowExclusiveLock);
  if (!HeapTupleIsValid(tuple)) {
    if (!missing_ok) elog(ERROR, "pax table %u could not be found", oid);

    pax_fs_name = NULL;
  } else {
    // no need to get relid from tuple
    pax_fs_name = (char *)palloc(50);
    sprintf(pax_fs_name, "pax_fast_sequences_%d", oid);
  }
  CPaxCloseFastSequenceTable(pax_fastsequence_rel, scan, tuple, 
                             RowExclusiveLock);

  return pax_fs_name;
}

namespace cbdb {

int32 CPaxGetFastSequences(Oid objid, bool increase) {
  CBDB_WRAP_START;
  { return ::CPaxGetFastSequences(objid, increase); }
  CBDB_WRAP_END;
  return -1;
}

}  // namespace cbdb
