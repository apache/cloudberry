#include "protos/yagpcc_set_service.pb.h"

#include "LogOps.h"
#include "LogSchema.h"

extern "C" {
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "cdb/cdbvars.h"
#include "commands/tablecmds.h"
#include "funcapi.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
}

void init_log() {
  Oid namespaceId;
  Oid relationId;
  ObjectAddress tableAddr;
  ObjectAddress schemaAddr;

  namespaceId = get_namespace_oid(schema_name.data(), false /* missing_ok */);

  /* Create table */
  relationId = heap_create_with_catalog(
      log_relname.data() /* relname */, namespaceId /* namespace */,
      0 /* tablespace */, InvalidOid /* relid */, InvalidOid /* reltype oid */,
      InvalidOid /* reloftypeid */, GetUserId() /* owner */, HEAP_TABLE_AM_OID,
      DescribeTuple() /* rel tuple */, NIL /* cooked_constraints */, RELKIND_RELATION,
      RELPERSISTENCE_PERMANENT, false /* shared_relation */, false /* mapped_relation */, ONCOMMIT_NOOP,
      NULL /* GP Policy */, (Datum)0 /* reloptions */, false /* use_user_acl */, true /* allow_system_table_mods */, true /* is_internal */,
      InvalidOid /* relrewrite */, NULL /* typaddress */,
      false /* valid_opts */);

  /* Make the table visible */
  CommandCounterIncrement();

  /* Record dependency of the table on the schema */
  if (OidIsValid(relationId) && OidIsValid(namespaceId)) {
    ObjectAddressSet(tableAddr, RelationRelationId, relationId);
    ObjectAddressSet(schemaAddr, NamespaceRelationId, namespaceId);

    /* Table can be dropped only via DROP EXTENSION */
    recordDependencyOn(&tableAddr, &schemaAddr, DEPENDENCY_EXTENSION);
  } else {
    ereport(NOTICE, (errmsg("YAGPCC failed to create log table or schema")));
  }

  /* Make changes visible */
  CommandCounterIncrement();
}

void insert_log(const yagpcc::SetQueryReq &req, bool utility) {
  Oid namespaceId;
  Oid relationId;
  Relation rel;
  HeapTuple tuple;

  /* Return if xact is not valid (needed for catalog lookups). */
  if (!IsTransactionState()) {
    return;
  }

  /* Return if extension was not loaded */
  namespaceId = get_namespace_oid(schema_name.data(), true /* missing_ok */);
  if (!OidIsValid(namespaceId)) {
    return;
  }

  /* Return if the table was not created yet */
  relationId = get_relname_relid(log_relname.data(), namespaceId);
  if (!OidIsValid(relationId)) {
    return;
  }

  bool nulls[natts_yagp_log];
  Datum values[natts_yagp_log];

  memset(nulls, true, sizeof(nulls));
  memset(values, 0, sizeof(values));

  extract_query_req(req, "", values, nulls);
  nulls[attnum_yagp_log_utility] = false;
  values[attnum_yagp_log_utility] = BoolGetDatum(utility);

  rel = heap_open(relationId, RowExclusiveLock);

  /* Insert the tuple as a frozen one to ensure it is logged even if txn rolls
   * back or aborts */
  tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
  frozen_heap_insert(rel, tuple);

  heap_freetuple(tuple);
  /* Keep lock on rel until end of xact */
  heap_close(rel, NoLock);

  /* Make changes visible */
  CommandCounterIncrement();
}

void truncate_log() {
  Oid namespaceId;
  Oid relationId;
  Relation relation;

  namespaceId = get_namespace_oid(schema_name.data(), false /* missing_ok */);
  relationId = get_relname_relid(log_relname.data(), namespaceId);

  relation = heap_open(relationId, AccessExclusiveLock);

  /* Truncate the main table */
  heap_truncate_one_rel(relation);

  /* Keep lock on rel until end of xact */
  heap_close(relation, NoLock);

  /* Make changes visible */
  CommandCounterIncrement();
}