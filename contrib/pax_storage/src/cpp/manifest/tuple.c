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
 * tuple.c
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/manifest/tuple.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "manifest.h"
#include "catalog/pax_catalog_columns.h"

/*
 * find field index in the tuple feild descriptor
 */
static int find_field_index(ManifestDesc tupledescriptor, const char *field_name) {
  int i;
  
  Assert(field_name);
  for (i = 0; i < tupledescriptor->numattrs; i++) {
    if (strcmp(tupledescriptor->attrs[i].field_name, field_name) == 0)
      return i;
  }

  return -1;
}

static ManifestHeap *getManifestHeap(ManifestRelation mrel) {
  if (!mrel->heap) {
    char *manifest_name = get_manifest_top_entrance(mrel->rel, NULL);
    build_manifest_relation(mrel, manifest_name);
  }
  return mrel->heap;
}

ManifestTuple make_empty_tuple(ManifestDesc desc) {
  ManifestTuple mtuple;
  int numattrs = desc->numattrs;

  mtuple = (ManifestTuple)palloc(sizeof(ManifestTupleData));
  mtuple->header.isdeleted = false;
  mtuple->header.isnulls = (bool *)palloc(numattrs * sizeof(bool));
  mtuple->data = (MetaValue *)palloc(numattrs * sizeof(MetaValue));
  for (int i = 0; i < numattrs; i++) {
    mtuple->data[i].field_name = desc->attrs[i].field_name;
  }

  memset(mtuple->header.isnulls, true, numattrs * sizeof(bool));

  return mtuple;
}

static void free_manifest_tuple(ManifestTuple tuple) {
  pfree(tuple->header.isnulls);
  pfree(tuple->data);
  pfree(tuple);
}

/*
 * construct a tuple from MetaValue array
 */
ManifestTuple manifesttuple_from_values(ManifestDescData *tupledescriptor,
                                        const MetaValue data[], int count) {
  ManifestTuple mtuple = make_empty_tuple(tupledescriptor);
  // not a effecient way to setup the tuple
  int i;
  for (i = 0; i < count; i++) {
    int idx = find_field_index(tupledescriptor, data[i].field_name);
    Assert(idx >= 0);
    Assert(strcmp(mtuple->data[idx].field_name, data[i].field_name) == 0);

    // actually need to copy the value here !!
    mtuple->data[idx].value = data[i].value;
    mtuple->header.isnulls[idx] = false;
  }
  return mtuple;
}

void markTupleDelete(ManifestTuple tuple) { tuple->header.isdeleted = true; }

static void copy_value(MetaFieldType typ, Datum *target, Datum src)
{
  switch (typ) {
    case Meta_Field_Type_Int:
    case Meta_Field_Type_Float:
    case Meta_Field_Type_Bool:
      *target = src;
      break;
    case Meta_Field_Type_String: {
      char *src_str = DatumGetCString(src);
      Assert(src_str);
      *target = CStringGetDatum(pstrdup(src_str));
    }
    break;
    // consider to do soft copy for bytes type data, it is too large
    case Meta_Field_Type_Bytes: {
      struct varlena *s = (struct varlena *)DatumGetPointer(src);
      if (!PointerIsValid(s))
        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                  errmsg("invalid Datum pointer")));

      size_t size = (size_t) VARSIZE_ANY(s);
      void *newdata = palloc(size);
      memcpy(newdata, DatumGetPointer(src), size);
      *target = PointerGetDatum(newdata);
    }
    break;
  default:
    Assert(false && "uncaught type");
    elog(ERROR, "uncaught type");
  }
}

/*
 * copy a manifest tuple with all its fields value, it is a deep copy, string
 * type data will copy its value, too
 */
ManifestTuple copy_tuple(ManifestTuple tuple, ManifestDesc tupledescriptor)
{
  ManifestTuple newtuple;
  int n = tupledescriptor->numattrs;
  
  newtuple = (ManifestTuple)palloc(sizeof(ManifestTupleData));
  newtuple->data =
      (MetaValue *)palloc(n * sizeof(MetaValue));

  newtuple->header = tuple->header;
  newtuple->header.isnulls =
      (bool *)palloc(tupledescriptor->numattrs * sizeof(bool));
  memcpy(newtuple->header.isnulls, tuple->header.isnulls,
         tupledescriptor->numattrs * sizeof(bool));
  memcpy(newtuple->data, tuple->data, n * sizeof(MetaValue));

  return newtuple;
}

/*
 * construct a new tuple base an old tuple with new value array
 */
ManifestTuple merge_tuple(ManifestTuple tuple, ManifestDesc tupledescriptor,
                          const MetaValue data[], int count) {
  ManifestTuple newtuple = copy_tuple(tuple, tupledescriptor);
  int i;

  for (i = 0; i < count; i++) {
    int idx = find_field_index(tupledescriptor, data[i].field_name);
    Assert(idx >= 0);
    Assert(strcmp(newtuple->data[idx].field_name, data[i].field_name) == 0);

    // actually need to copy the value here !!
    MetaFieldType typ = Get_DESCRIPTOR_ATTRIBUTE_TYPE(tupledescriptor, idx);
    
    copy_value(typ, &newtuple->data[idx].value, data[i].value);

    newtuple->header.isnulls[idx] = false;
  }

  return newtuple;
}

/*
 *get the field value for a tuple, the value returned has its memory allocated
 * in manifest heap, the memory space of the value will be cleared after
 * transaction end.
 */
Datum get_manifesttuple_value(ManifestTuple tuple, ManifestRelation mfrel,
                             const char *field_name, bool *isnull) {
  ManifestDesc desc = mfrel->desc;
  int idx = find_field_index(desc, field_name);
  if (idx < 0) // should print the error??
     elog(ERROR, "get manifest tuple no field_name found");

  *isnull = tuple->header.isnulls[idx];
  return tuple->data[idx].value;
}

void manifest_free_tuple(ManifestTuple tuple){
}

/*
 * update a exist tuple to new one, the old tuple will not be deleted as just
 * mark as deleted, and the new tuple will append to the heap list
 */
static void manifest_update_internal(ManifestRelation mrel,
                                     ManifestTuple oldtuple,
                                     const MetaValue data[],
                                     int count) {
  MemoryContext oldctx;
  ManifestHeap *mheap;
  ManifestTuple newtuple;

  oldctx = MemoryContextSwitchTo(mrel->mctx);
  mheap = getManifestHeap(mrel);

  newtuple = merge_tuple(oldtuple, mrel->desc, data, count);

  markTupleDelete(oldtuple);

  mheap->manifesttuples = lappend(mheap->manifesttuples, newtuple);
  mrel->dirty = true;

  MemoryContextSwitchTo(oldctx);
}

void manifest_update(ManifestRelation mrel, int block, const MetaValue data[],
                     int count) {
  ManifestHeap *mheap;
  ManifestTuple tuple;
  MemoryContext oldctx;

  oldctx = MemoryContextSwitchTo(mrel->mctx);

  mheap = getManifestHeap(mrel);
  for (int i = 0; i < list_length(mheap->manifesttuples); i++) {
    Datum value;
    bool isnull;

    tuple = (ManifestTuple)list_nth(mheap->manifesttuples, i);
    if (tuple->header.isdeleted) continue;

    value = get_manifesttuple_value(tuple, mrel, PAX_AUX_PTBLOCKNAME, &isnull);
    Assert(!isnull);
    if (DatumGetInt32(value) == block) {
      manifest_update_internal(mrel, tuple, data, count);
      break;
    }
  }
  MemoryContextSwitchTo(oldctx);
}

void manifest_delete(ManifestRelation mrel, int block) {
  ManifestHeap *mheap;
  ManifestTuple tuple;
  MemoryContext oldctx;

  oldctx = MemoryContextSwitchTo(mrel->mctx);
  mheap = getManifestHeap(mrel);
  for (int i = 0; i < list_length(mheap->manifesttuples); i++) {
    Datum value;
    bool isnull;

    tuple = (ManifestTuple)list_nth(mheap->manifesttuples, i);
    if (tuple->header.isdeleted) continue;

    value = get_manifesttuple_value(tuple, mrel, PAX_AUX_PTBLOCKNAME, &isnull);
    Assert(!isnull);
    if (DatumGetInt32(value) == block) {
      markTupleDelete(tuple);
      mrel->dirty = true;
      break;
    }
  }
  MemoryContextSwitchTo(oldctx);
}

/**
 * construct a new manifest tuple and append it to manifest heap list
 * NOTE: manifest_insert will only insert a placeholder for micro partition,
 * It's a temp tuple and will be overwritten by the next update.
 * So, it will not write xlog here.
 */
void manifest_insert(ManifestRelation mrel, const MetaValue data[],
                     int count) {
  MemoryContext oldctx;
  ManifestHeap *mheap;
  ManifestTuple mtuple;
  Relation rel;
  
  oldctx = MemoryContextSwitchTo(mrel->mctx);

  mheap = getManifestHeap(mrel);
  mtuple = manifesttuple_from_values(mrel->desc, data, count);

  // appned the new tuple to the end
  mheap->manifesttuples = lappend(mheap->manifesttuples, mtuple);
  mrel->dirty = true;

  MemoryContextSwitchTo(oldctx);
}
