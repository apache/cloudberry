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
 * manifest.c
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/manifest/manifest.c
 *
 *-------------------------------------------------------------------------
 */

#include <errno.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#include "postgres.h"
#include "nodes/pg_list.h"
#include "storage/relfilenode.h"

#include "comm/pax_rel.h"
#include "manifest.h"
#include "tuple.h"
#include "yyjson.h"

#include "access/relation.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/tupmacs.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pax_catalog_columns.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/pg_opclass_d.h"
#include "catalog/dependency.h"
#include "catalog/storage.h"
#include "fmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#include "utils/bytea.h"
#include "utils/relfilenodemap.h"
#include "utils/lsyscache.h"

#include "cdb/cdbvars.h"

// headers from pax
#include "catalog/pax_fastsequence.h"
#include "manifest_wrapper.h"


#define PAX_MICROPARTITION_DIR_POSTFIX "_pax"

/*
 * issues:
 * there is a issue for our cache memory, we setup two cache, one is for heap
 * and the other is for relation, the HTAB global variable initialization should
 * happend after the null check, but when the transaction exit with exception,
 * we have no chance to clear the HTAB global variables, so that we have no way
 * to do the right initialization.
 *
 * extend descriptor fields actually should only appended on init, we'd better
 * not provide a method to extend the fields at any time.
 */

MemoryContext manifest_mcontext = NULL;
static ManifestDescData *manifest_descriptors = NULL;

#define INVALID_MANIFEST_VERSION ((uint32)0)
#define INIT_MANIFEST_VERSION ((uint32)1)
#define INIT_LAST_MANIFEST ""

static void *private_malloc(void *ctx, size_t size);
static void *private_realloc(void *ctx, void *ptr, size_t old_size,
                             size_t size);
static void private_free(void *ctx, void *ptr);
static const yyjson_alc PG_ALC = {private_malloc, private_realloc, private_free,
                                  NULL};

static void manifest_flush_single_relation(ManifestRelation mrel);

#define AUX_PREFIX "pg_manifest"

static ManifestDesc init_system_descriptors() {
  int numattrs = 7;
  ManifestDesc desc =
      malloc(sizeof(ManifestDescData) + numattrs * sizeof(ManifestAttribute));

  desc->attrs[0].field_name = PAX_AUX_PTBLOCKNAME;
  desc->attrs[0].field_type = Meta_Field_Type_Int;
  desc->attrs[0].deflt = Int32GetDatum(-1);
  desc->attrs[1].field_name = PAX_AUX_PTBLOCKSIZE;
  desc->attrs[1].field_type = Meta_Field_Type_Int;
  desc->attrs[1].deflt = Int32GetDatum(0);
  desc->attrs[2].field_name = PAX_AUX_PTTUPCOUNT;
  desc->attrs[2].field_type = Meta_Field_Type_Int;
  desc->attrs[2].deflt = Int32GetDatum(0);
  desc->attrs[3].field_name = PAX_AUX_PTEXISTEXTTOAST;
  desc->attrs[3].field_type = Meta_Field_Type_Bool;
  desc->attrs[3].deflt = BoolGetDatum(false);
  desc->attrs[4].field_name = PAX_AUX_PTVISIMAPNAME;
  desc->attrs[4].field_type = Meta_Field_Type_String;
  desc->attrs[4].deflt = CStringGetDatum(0);
  desc->attrs[5].field_name = PAX_AUX_PTISCLUSTERED;
  desc->attrs[5].field_type = Meta_Field_Type_Bool;
  desc->attrs[5].deflt = BoolGetDatum(false);
  desc->attrs[6].field_name = PAX_AUX_PTSTATISITICS;
  desc->attrs[6].field_type = Meta_Field_Type_Bytes;
  desc->attrs[6].deflt = (Datum)NULL;

  desc->numattrs = numattrs;

  return desc;
}

static void *private_malloc(void *ctx, size_t size) {
  (void)ctx;
  return AllocSizeIsValid(size) ? palloc(size)
                                : palloc_extended(size, MCXT_ALLOC_HUGE);
}

static void *private_realloc(void *ctx, void *ptr, size_t old_size,
                             size_t size) {
  (void)ctx;
  (void)old_size;
  return AllocSizeIsValid(size) ? repalloc(ptr, size)
                                : repalloc_huge(ptr, size);
}

static void private_free(void *ctx, void *ptr) {
  (void)ctx;
  pfree(ptr);
}

/*
 * get the aux table name and its index name
 * the space for the two string be pre-allocated with at least NAMEDATALEN len
 */
static void get_aux_table_name_info(Oid relid, char *tname) {
  Assert(tname);
  snprintf(tname, NAMEDATALEN, "%s_%u", AUX_PREFIX, relid);
}

static char *
serialize_bytea(bytea *stats)
{
  char *summary = NULL;

  if (stats)
  {
    Datum d;
    int oldbytea_output = bytea_output;

    bytea_output = BYTEA_OUTPUT_HEX;
    d = DirectFunctionCall1(byteaout, PointerGetDatum(stats));
    summary = DatumGetCString(d);
    bytea_output = oldbytea_output;
  }

  return summary;
}

static bytea *
deserialize_bytea(const char *summary)
{
  bytea  *stats = NULL;
  size_t summarylen = strlen(summary);

  if (summarylen < 2 || !AllocSizeIsValid((summarylen - 2) / 2 + VARHDRSZ))
  {
    ereport(NOTICE,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
             errmsg("deserialize bytea failed, bytea length %zu", summarylen)));

    return NULL;
  }

  /* we should only meet BYTEA_OUTPUT_HEX format msg */
  if (!(summary[0] == '\\' && summary[1] == 'x'))
  {
    ereport(NOTICE,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
             errmsg("deserialize bytea which is not BYTEA_OUTPUT_HEX format")));

    return NULL;
  }

  Datum dec = DirectFunctionCall1(byteain, CStringGetDatum(summary));
  stats = DatumGetByteaP(dec);

  return stats;
}

static void manifestvalue_to_json(yyjson_mut_doc *doc, yyjson_mut_val *obj,
                                  MetaFieldType type, const char *field_name,
                                  Datum value) {
  switch (type) {
  case Meta_Field_Type_Int:
    yyjson_mut_obj_add_int(doc, obj, field_name, DatumGetInt32(value));
    break;
  case Meta_Field_Type_String:
    yyjson_mut_obj_add_str(doc, obj, field_name, DatumGetCString(value));
    break;
  case Meta_Field_Type_Float:
    yyjson_mut_obj_add_real(doc, obj, field_name, DatumGetFloat8(value));
    break;
  case Meta_Field_Type_Bool:
    yyjson_mut_obj_add_bool(doc, obj, field_name, DatumGetBool(value));
    break;
  case Meta_Field_Type_Bytes:
     {
       // encode the binary data to text and save to json
       char *enc = serialize_bytea((bytea*)DatumGetPointer(value));
       yyjson_mut_obj_add_str(doc, obj, field_name, enc);
     }
    break;
  default:
    break;
  }
}

Datum json_to_manifestvalue(yyjson_val *type_val, MetaFieldType typ) {
  Datum value;

  switch (typ) {
  case Meta_Field_Type_Int:
    return Int32GetDatum(yyjson_get_int(type_val));
  case Meta_Field_Type_String:
    return CStringGetDatum(pstrdup(yyjson_get_str(type_val)));
  case Meta_Field_Type_Float:
    return Float8GetDatum(yyjson_get_real(type_val));
  case Meta_Field_Type_Bool:
    return BoolGetDatum(yyjson_get_bool(type_val));
  case Meta_Field_Type_Bytes:
     {
       const char *source = yyjson_get_str(type_val);
       return PointerGetDatum(deserialize_bytea(source));
     }
  default:
    Assert(false && "uncaught type");
  }
  return (Datum)NULL;
}

static yyjson_mut_val *manifesttuple_to_json(yyjson_mut_doc *doc,
                                             ManifestDescData *desc,
                                             ManifestTuple tuple) {
  yyjson_mut_val *obj = yyjson_mut_obj(doc);
  int i = 0;

  for (; i < desc->numattrs; i++) {
    const char *field_name = Get_DESCRIPTOR_ATTRIBUTE_NAME(desc, i);
    if (tuple->header.isnulls[i])
      continue; // maybe
    manifestvalue_to_json(doc, obj, Get_DESCRIPTOR_ATTRIBUTE_TYPE(desc, i),
                          field_name, tuple->data[i].value);
  }
  return obj;
}

static const char *manifest_to_json(ManifestHeap *m, size_t *len) {
  ListCell *lc;
  yyjson_write_err err;

  yyjson_mut_doc *doc = yyjson_mut_doc_new(&PG_ALC);
  yyjson_mut_val *root = yyjson_mut_obj(doc);
  yyjson_mut_doc_set_root(doc, root);

  yyjson_mut_obj_add_uint(doc, root, "version", m->version);
  yyjson_mut_obj_add_uint(doc, root, "last_version", m->last_version);
  yyjson_mut_obj_add_str(doc, root, "last_manifest", m->last_manifest);
  yyjson_mut_val *array = yyjson_mut_obj_add_arr(doc, root, "datametas");
  foreach (lc, m->manifesttuples) {
    ManifestTuple tuple = (ManifestTuple)lfirst(lc);
    if (tuple->header.isdeleted)
      continue;
    yyjson_mut_arr_append(array, manifesttuple_to_json(doc, m->desc, tuple));
  }

  const char *json = yyjson_mut_write_opts(doc, 0, &PG_ALC, len, &err);
  if (err.code != YYJSON_WRITE_SUCCESS)
    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("generate json(%s) for mainfest failed, %s", m->cur_manifest, err.msg)));

  return json;
}

static bool json_to_manifestheap(ManifestHeap *m, char *buf, int len) {
  size_t idx, max;
  yyjson_val *datameta, *add, *remove;
  const char *summary;
  yyjson_read_err err;

  yyjson_doc *doc =
      yyjson_read_opts(buf, len, YYJSON_READ_INSITU, &PG_ALC, &err);
  if (err.code != YYJSON_READ_SUCCESS)
    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("read manifest from json failed, %s", err.msg)));

  yyjson_val *root = yyjson_doc_get_root(doc);

  m->version = yyjson_get_uint(yyjson_obj_get(root, "version"));
  m->last_version = yyjson_get_uint(yyjson_obj_get(root, "last_version"));
  m->last_manifest =
      (char *)yyjson_get_str(yyjson_obj_get(root, "last_manifest"));
  yyjson_val *datametas = yyjson_obj_get(root, "datametas");
  yyjson_arr_foreach(datametas, idx, max, datameta) {
    ManifestDescData *desc = m->desc;
    ManifestTuple tuple = make_empty_tuple(desc);

    int i = 0;
    for (; i < desc->numattrs; i++) {
      const char *field_name = Get_DESCRIPTOR_ATTRIBUTE_NAME(desc, i);
      yyjson_val *type_val = yyjson_obj_get(datameta, field_name);
      tuple->data[i].field_name = field_name;
      if (!type_val)
      {
         tuple->header.isnulls[i] = true;
         continue;
      }

      Datum value = json_to_manifestvalue(
          type_val, Get_DESCRIPTOR_ATTRIBUTE_TYPE(desc, i));
      tuple->data[i].value = value;
      tuple->header.isnulls[i] = false;
    }
    m->manifesttuples = lappend(m->manifesttuples, tuple);
  }

  return true;
}

/*
 * generate a manifest file path for every relation, curerntly the path
 * building logic is base on the uuid, we put some part of uuid string
 * in as directory prefix in order to do decentralized access low cost
 * the access disk performance for manifest file, this is much valueble
 * on cloud storage.
 * FIXME: the length of the file path might be larger than 64 bytes,
 * which disallowed by pax xlog.
 */
static char *make_manifest_name() {
  Datum uuid_d = OidFunctionCall0(F_GEN_RANDOM_UUID);
  char *uuid = DatumGetCString(OidFunctionCall1(F_UUID_OUT, uuid_d));

  char *path = psprintf("%s_%u.meta", uuid, GetCurrentTransactionId());
  pfree(uuid);
  return path;
}

static inline Relation manifest_open_aux_relation(Oid relid, LOCKMODE lockmode) {
  char aux_name[NAMEDATALEN];
  Oid aux_oid;

  get_aux_table_name_info(relid, aux_name);
  aux_oid = get_relname_relid(aux_name, PG_EXTAUX_NAMESPACE);
  return table_open(aux_oid, lockmode);
}

static inline void manifest_close_aux_relation(Relation aux_rel, LOCKMODE lockmode) {
  table_close(aux_rel, lockmode);
}

/*
 * serialize the manifest heap memory struct to a disk file
 */
void serialize_manifest(Relation rel, ManifestHeap *manifest, char *manifest_paxdir, char *manifest_name) {
  size_t len;
  char *manifest_path;
  const char *json_str;
  
  manifest_path = psprintf("%s/%s", manifest_paxdir, manifest_name);
  json_str = manifest_to_json(manifest, &len);
  paxc_store_file(manifest_path, json_str, len);

  // update WAL log for new manifest file
  paxc_wal_insert_if_required(rel, manifest_name, json_str, len, 0);

  pfree(manifest_path);
  pfree((void *)json_str);
}

/* functions to update the top entrance for manifest entry
 * currently the manifest top entrance was designed to stored in the heap
 * catalog table, when we sync the new version of manifest data to disk
 * we need refresh the new manifest file path in the top entrance table
 */
void update_manifest_top_entrance(Relation rel, const char *manifest_path) {
  Oid aux_oid;
  Relation aux_rel;
  TupleDesc desc;
  SysScanDesc scan;
  HeapTuple oldtuple;
  HeapTuple newtuple;
  Datum values[1];
  bool nulls[1];

  aux_rel = manifest_open_aux_relation(RelationGetRelid(rel), ExclusiveLock);
  desc = RelationGetDescr(aux_rel);

  values[0] = CStringGetTextDatum(manifest_path);
  nulls[0] = false;
  newtuple = heap_form_tuple(desc, values, nulls);

  scan = systable_beginscan(aux_rel, InvalidOid, false, NULL, 0, NULL);
  oldtuple = systable_getnext(scan);
  if (HeapTupleIsValid(oldtuple)) {
    CatalogTupleUpdate(aux_rel, &oldtuple->t_self, newtuple);
  } else {
    // no tuple exists
    CatalogTupleInsert(aux_rel, newtuple);
  }
  heap_freetuple(newtuple);
  systable_endscan(scan);
  manifest_close_aux_relation(aux_rel, NoLock);

  CommandCounterIncrement();
}

// return null or palloced manifest_name
char *get_manifest_top_entrance(Relation rel, Snapshot snapshot) {
  Relation aux_rel;
  SysScanDesc scan;
  HeapTuple tuple;
  char *path = NULL;
  bool isnull;

  aux_rel = manifest_open_aux_relation(RelationGetRelid(rel), AccessShareLock);
  // currently do not use index scan, will do it later
  scan = systable_beginscan(aux_rel, InvalidOid, false, snapshot, 0, NULL);
  // there should be only one valid tuple
  tuple = systable_getnext(scan);
  if (!HeapTupleIsValid(tuple)) goto out;

  path = pstrdup(text_to_cstring(
    DatumGetTextP(heap_getattr(tuple, 1, RelationGetDescr(aux_rel),
                               &isnull))));
  Assert(!isnull && path);
  // at most one tuple is valid.
  Assert(!HeapTupleIsValid(systable_getnext(scan)));

out:
  systable_endscan(scan);
  manifest_close_aux_relation(aux_rel, AccessShareLock);

  return path;
}

/*
 * function ti init all the internal resources
 * - manfiest tuple descriptors
 * - set file system operation implementations (multiple filesytem support)
 * - log functions (multiple log print function support)
 */
ManifestDesc manifest_init() {
  manifest_mcontext = AllocSetContextCreate(TopMemoryContext, "manifest-context", ALLOCSET_DEFAULT_SIZES);
  ManifestDesc desc = init_system_descriptors();
  manifest_descriptors = desc;

  return desc;
}

/*
 * create manifest auxiliary heap table to store a manifest top entrance path.
 * normally the auxiliary table is created when create a pax table, and of
 * course will be  deleted when removing a pax table. the key of the auxiliary
 * table is relid, and also there is a depend reference to its base Relation.
 *
 * notice that we currenlty create aux table in the pax namespace
 */
Oid create_manifest_auxiliary_heap(Relation parentrel) {
  Oid aux_oid = InvalidOid;
  TupleDesc tupdesc;
  char aux_relname[NAMEDATALEN];

  get_aux_table_name_info(RelationGetRelid(parentrel), aux_relname);
  Assert(!OidIsValid(get_relname_relid(aux_relname, PG_EXTAUX_NAMESPACE)));

  tupdesc = CreateTemplateTupleDesc(1);
  TupleDescInitEntry(tupdesc, (AttrNumber) 1,
                     "path",
                     TEXTOID,
                     -1, 0);

  aux_oid = heap_create_with_catalog(aux_relname,
                                     PG_EXTAUX_NAMESPACE,
                                     InvalidOid,
                                     InvalidOid,
                                     InvalidOid,
                                     InvalidOid,
                                     parentrel->rd_rel->relowner,
                                     HEAP_TABLE_AM_OID,
                                     tupdesc,
                                     NIL,
                                     RELKIND_RELATION,
                                     parentrel->rd_rel->relpersistence,
                                     parentrel->rd_rel->relisshared,
                                     RelationIsMapped(parentrel),
                                     ONCOMMIT_NOOP,
                                     NULL, /* GP Policy */
                                     (Datum) 0,
                                     /* use_user_acl */ false,
                                     true,
                                     true,
                                     InvalidOid,
                                     NULL, /* typeaddress */
                                     /* valid_opts */ false);

  {
    // record the depencency, so the auxiliary will be deleted automatically.
    ObjectAddress baseobject;
    ObjectAddress manifestobject;

    baseobject.classId = RelationRelationId;
    baseobject.objectId = RelationGetRelid(parentrel);
    baseobject.objectSubId = 0;
    manifestobject.classId = RelationRelationId;
    manifestobject.objectId = aux_oid;
    manifestobject.objectSubId = 0;

    recordDependencyOn(&manifestobject, &baseobject, DEPENDENCY_INTERNAL);
  }

  CommandCounterIncrement();
  return aux_oid;
}

/*
 * called in the create table context to create an empty initial manifest
 * file, so that avoid checking the manifest existence in many scenarios
 *
 * creating initial manifest do not require a special memoryctx, all the work
 * will be done in same execution stack context.
 *
 * function will create a auxiliary manifest table to store the manifest top
 * entrance path, also create a depend entry with relid
 */
void manifest_create(Relation rel, RelFileNode relnode) {
  char aux_relname[NAMEDATALEN];
  ReindexParams reindex_params = {0};
  Relation aux_rel;
  Oid aux_oid;
  bool exists;

  get_aux_table_name_info(RelationGetRelid(rel), aux_relname);
  aux_oid = get_relname_relid(aux_relname, PG_EXTAUX_NAMESPACE);
  /*
   * to tell it is a truncate or create table ddl
   * if the aux table is exists, it is the truncate
   */
  exists = OidIsValid(aux_oid);
  if (exists) {
    // table exists, do truncate, needs rollback.
    aux_rel = table_open(aux_oid, AccessExclusiveLock);
    RelationSetNewRelfilenode(aux_rel, aux_rel->rd_rel->relpersistence);
  } else {
    // create table
    aux_oid = create_manifest_auxiliary_heap(rel);
    Assert(OidIsValid(aux_oid));
    aux_rel = table_open(aux_oid, AccessExclusiveLock);
  }
  paxc_create_pax_directory(rel, relnode);

  table_close(aux_rel, NoLock);

  // initialize or reset the fast sequence number
  CPaxInitializeFastSequenceEntry(
      RelationGetRelid(rel),
      exists ? FASTSEQUENCE_INIT_TYPE_UPDATE : FASTSEQUENCE_INIT_TYPE_CREATE,
      0);
}

/*
 * for non-transactional truncate
 * all the data file and old manifest will be deleted
 */
void manifest_truncate(Relation rel) {
  Relation aux_rel;
  SysScanDesc sscan;
  TupleDesc desc;
  HeapTuple tuple;

  aux_rel = manifest_open_aux_relation(RelationGetRelid(rel), AccessExclusiveLock);
  desc = RelationGetDescr(aux_rel);

  sscan = systable_beginscan(aux_rel, InvalidOid, false, NULL, 0, NULL);
  while (HeapTupleIsValid((tuple = systable_getnext(sscan)))) {
    char *path;
    Datum datum;
    bool isnull;

    datum = heap_getattr(tuple, 1, desc, &isnull);
    Assert(!isnull);
    path = TextDatumGetCString(datum);

    if (unlink(path) == -1 && errno != ENOENT)
      elog(ERROR, "failed to remove file '%s'", path);
    pfree(path);
  }
  systable_endscan(sscan);

  heap_truncate_one_rel(aux_rel);
  manifest_close_aux_relation(aux_rel, NoLock);

  CPaxInitializeFastSequenceEntry(RelationGetRelid(rel),
                                  FASTSEQUENCE_INIT_TYPE_INPLACE,
                                  0);

  CommandCounterIncrement();

  if (paxc_need_wal(rel))
    paxc_wal_truncate_directory(rel->rd_node);
}

struct manifest_read_context {
  ManifestHeap *mheap;
  char *toppath;
};

static void manifest_read_cb(const void *ptr, size_t size, void *opaque) {
  struct manifest_read_context *ctx;
  ctx = (struct manifest_read_context *)opaque;
  bool ok = json_to_manifestheap(ctx->mheap, (char *)ptr, size);
  if (!ok)
     elog(ERROR, "fail to parse manifest file %s", ctx->toppath);
}
/*
 * build manifest heap instance from disk file
 */
void build_manifest_heap(const char *paxdir, char *manifest_name, ManifestHeap *mheap) {
  struct manifest_read_context context;
  char toppath[2048];

  mheap->desc = manifest_descriptors;
  mheap->cur_manifest = manifest_name;
  mheap->manifesttuples = NIL;
  if (!manifest_name) return;

  snprintf(toppath, sizeof(toppath), "%s/%s", paxdir, manifest_name);

  context.mheap = mheap;
  context.toppath = toppath;
  paxc_read_all(toppath, manifest_read_cb, &context);
}

/*
 * build the ManifestRelation, initialize the descriptor
 * build the manifest heap for the first call
 *
 * observision: consider to manifest heap seperate from the relation, heap stand
 * for the storage format, and relation stand for jsut a handle to do some
 * access control, but currently seems it is a overengineering.
 *
 * inh: consider to create a manifest tuple count "shortcut"" to relation
 * struct, since the valid tuple count cannot simply be accessed by list_length
 */
void build_manifest_relation(ManifestRelation mrel, char *manifest_name) {
  ManifestHeap *heap;
  MemoryContext oldcxt = MemoryContextSwitchTo(mrel->mctx);
  Assert(mrel->heap == NULL);

  heap = palloc0(sizeof(ManifestHeap));
  build_manifest_heap(mrel->paxdir, manifest_name, heap);
  mrel->desc = heap->desc;
  mrel->heap = heap;

  MemoryContextSwitchTo(oldcxt);
}

/*
 * return a maniest operation handle, the handle is opened and stored
 * temporarily in cache, it will build the manfiest heap data for the first
 * time call ManifestRelation has its memory allocated in TopTransactionContext
 * the memory will not be cleared when we call the manifest_close(), it only
 * manage reference count, the manifest_commit will clear the mamory.
 *
 * observision: we can consider put the ManifestRelation data in share memory
 * and not clear it any more util dropping this table, and call a function
 * like invalidate* to refresh the content. it will not do a manifest data
 * reload for every transactionAll these depend on the performance requirement
 */
ManifestRelation manifest_open(Relation rel) {
  ManifestRelation mrel;
  MemoryContext oldctx;
  MemoryContext mctx;

  mctx = AllocSetContextCreate(CurrentMemoryContext, "manifest-relation-context", ALLOCSET_DEFAULT_SIZES);
  oldctx = MemoryContextSwitchTo(mctx);
  mrel = palloc0(sizeof(ManifestRelationData));

  {
    mrel->rel = rel;
    mrel->mctx = mctx;
    mrel->desc = manifest_descriptors;
    mrel->heap = NULL;
    mrel->dirty = false;
    mrel->paxdir = paxc_get_pax_dir(rel->rd_node, rel->rd_backend);
  }
  MemoryContextSwitchTo(oldctx);
  return mrel;
}

static void manifest_flush_single_relation(ManifestRelation mrel) {
  char *manifest_name;
  ManifestHeap *mheap;

  Assert(mrel && mrel->heap);

  mheap = mrel->heap;
  mheap->last_version = mheap->version;
  mheap->version = mheap->version + 1;
  mheap->last_manifest = mheap->cur_manifest;

  manifest_name = make_manifest_name();

  serialize_manifest(mrel->rel, mheap, mrel->paxdir, manifest_name);
  update_manifest_top_entrance(mrel->rel, manifest_name);

  pfree(manifest_name);
}

void manifest_close(ManifestRelation mrel) {
  MemoryContext oldctx;
  MemoryContext mctx;

  Assert(mrel && mrel->mctx);
  mctx = mrel->mctx;
  oldctx = MemoryContextSwitchTo(mctx);
  if (mrel->dirty) {
    manifest_flush_single_relation(mrel);
  }
  MemoryContextSwitchTo(oldctx);
  MemoryContextDelete(mctx);
}

void manifest_swap_table(Oid relid1, Oid relid2,
                         TransactionId frozen_xid,
                         MultiXactId cutoff_multi) {
  Relation aux_rel1;
  Relation aux_rel2;

  aux_rel1 = manifest_open_aux_relation(relid1, AccessExclusiveLock);
  aux_rel2 = manifest_open_aux_relation(relid2, AccessExclusiveLock);

  swap_relation_files(RelationGetRelid(aux_rel1),
                      RelationGetRelid(aux_rel2), 
                      false, true, true, true,
                      frozen_xid, cutoff_multi, NULL);

  /* swap fast seq */
  {
    int32 seqno1, seqno2;

    seqno1 = CPaxGetFastSequences(relid1, false);
    seqno2 = CPaxGetFastSequences(relid2, false);

    if (seqno1 != seqno2) {
      CPaxInitializeFastSequenceEntry(relid1, FASTSEQUENCE_INIT_TYPE_UPDATE,
                                      seqno2);
      CPaxInitializeFastSequenceEntry(relid2, FASTSEQUENCE_INIT_TYPE_UPDATE,
                                      seqno1);
    }
  }
  SIMPLE_FAULT_INJECTOR("pax_finish_swap_fast_fastsequence");

  manifest_close_aux_relation(aux_rel1, NoLock);
  manifest_close_aux_relation(aux_rel2, NoLock);
}
