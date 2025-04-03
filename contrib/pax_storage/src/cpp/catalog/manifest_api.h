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
 * manifest_api.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/catalog/manifest_api.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

/*
 * abstract interface of hashdata manifest
 */

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
/**
 * for all the interface function, we only handle the error raised by
 * pg error, so library could utilize the postgres memorycontxt to do
 * memory management.
 */

/* hashdata manifest support following data types for fields */
typedef enum MetaFieldType
{
  Meta_Field_Type_Int = 1,
  Meta_Field_Type_Uint,
  Meta_Field_Type_String,
  Meta_Field_Type_Float,
  Meta_Field_Type_Bool,
  Meta_Field_Type_Bytes
} MetaFieldType;


typedef struct MetaAttribute
{
  const char *field_name;
  MetaFieldType field_type;
  Datum deflt;
} MetaAttribute;

typedef struct ManifestDescData
{
  int numattrs;
  MetaAttribute attrs[FLEXIBLE_ARRAY_MEMBER];
} ManifestDescData;

typedef ManifestDescData *ManifestDesc;

typedef struct MetaValue
{
  const char *field_name;
  Datum value;
} MetaValue;

/* some forward declaration */
/*
 * define the handle returned by manifest_open for operating manifest CRUD
 */
#ifndef USE_OWN_MANIFEST_RELATION
typedef struct ManifestRelationData ManifestRelationData;
typedef ManifestRelationData *ManifestRelation;
#endif

/*
 * define the manifest tuple identifier returned by getnext and insert interface
 * the handle will be provided for update/delete operation
 */
#ifndef USE_OWN_MANIFEST_TUPLE
typedef struct ManifestTupleData ManifestTupleData;
typedef ManifestTupleData *ManifestTuple;
#endif

/*
 * define manifest scan iterator for getting next tuple
 */
#ifndef USE_OWN_MANIFEST_SCAN
typedef struct ManifestScanData ManifestScanData;
typedef ManifestScanData *ManifestScan;
#endif

/*
 * define the scan key for scanning the manifest tuple
 */
typedef struct RelationData *Relation;
typedef struct SnapshotData *Snapshot;
typedef struct RelFileNode RelFileNode;


/*
 * initialize internal resources
 */
ManifestDesc manifest_init();

/**
 * create a new relfilenode for the pax table when creating the table or
 * truncate the table.
 * The action made by this function should be rolled back if the transaction
 * is cancelled.
 * Argument:
 *   relnode - the relnode of the pax table which is composed with
 *             tablespaceId, ReiFileNodeId, databaseId
 */
void manifest_create(Relation rel, RelFileNode newrelnode);

/**
 * Clear all micro partitions in the current version in non-transaction
 * When the current version is temporary(like newly created table),
 * truncating a table doesn't require rollback.
 * This function will remove all files in the current relnode version.
 */
void manifest_truncate(Relation pax_rel);


/**
 * Open and get the manifest operation handle. The returned handle should
 * be closed by manifest_close() normally. All resources should be released
 * if an error is raised that will not call manifest_close().
 */
ManifestRelation manifest_open(Relation pax_rel);

/**
 * close the opened manifest handle, the closed manifest handle cannot
 * be used anymore.
 */
void manifest_close(ManifestRelation mfrel);

/**
 * create a manifest tuple scan iterator with a scan key.
 * if the key is provided with NULL, the scan will just iterate all over manifest
 * tuples the returned ManifestScanData instance is created internally, and will be
 * release when calling manifest_endscan
 * Arguments:
 * mfrel - the manifest operation handle which returned by manifest_open interface
 * key - define search key
 */
ManifestScan manifest_beginscan(ManifestRelation mfrel, Snapshot snapshot);

/**
 * clean up a scan iterator, release all resources including the scan object itself.
 */
void manifest_endscan(ManifestScan scan);

/**
 * return a manifest tuple from scan iterator, the memory for returned manifest tuple
 * allocated by library, and the memory will be released by transaction commit.
 * the interface support parallel processing, since the memory for all the manifest
 * tuples has been all allocated at the init, the only posision of the iterator was
 * protected by atomic operation.
 * Argument:
 * scan - scan iterator created by manifest_beginscan
 * context - a extended filter object context
 */
ManifestTuple manifest_getnext(ManifestScan scan, void *context);

/**
 * insert a new manifest tuple with a set of given name-value pairs,
 * Argument:
 * mrel - manifest operation handle which is returned by manifest_open
 * data - an array of a name-value pair to setup a manifest tuple
 * count - the name-value array size
 *
 * interface return a manifest tuple instance which has been inserted
 * to manifest.
 *
 * the interface call cannot support parallel processing currently
 * the given name-value pairs will be copied to library internally, and
 * of course the user can release these space after calling this function
 * 
 * notice that the inserted tuple will be synced to disk when only after
 * call the manifest_commit.
 */
void manifest_insert(ManifestRelation mrel, const MetaValue data[],
                     int count);

/**
 * update a manifest tuple with a set of given name-value pairs,
 * Argument:
 * mrel - manifest operation handle which is returned by manifest_open
 * data - an array of a name-value pair to setup a manifest tuple
 * count - the name-value array size
 *
 * update will not directly update the old record, and just create a new
 * version of record with new values instead and old version will not be
 * seen. the transaction commit will remove the invisible record finally.
 *
 * the interface call cannot support parallel processing currently
 * the given name-value pairs will be copied to library internally, and
 * of course the user can release these space after calling this function
 * 
 * notice that the inserted tuple will be synced to disk when only after
 * call the manifest_commit.
 */
void manifest_update(ManifestRelation mrel, int block, const MetaValue data[],
                     int count);
/**
 * delete a manifest tuple
 * Argument:
 * mrel - manifest operation handle which is returned by manifest_open
 * tuple - manifest tuple to delete
 *
 * the interface call cannot support parallel processing currently
 * and the library actually will delete the given tuple and just
 * mark it invisible instead, the transaction commit will remove the
 * invisible record finally.
 *
 * the interface call cannot support parallel processing currently
 */
void manifest_delete(ManifestRelation mrel, int block);


/**
 * get a field value from a manifest tuple by a given field name
 * Arguments:
 * tuple - manifest tuple which get from manifest interface
 * mrel - manifest operation handle
 * field_name - the field name to get the value for
 * value - the datum where value stored
 * isnull - indicate the value is null
 *
 * the returnd value's memory space is allocated in library, and will be released
 * by transaction_commit, caller need to to free it.
 */
Datum get_manifesttuple_value(ManifestTuple tuple, ManifestRelation mrel,
                              const char* field_name, bool *isnull);

/**
 * get a ManifestTuple by a given block number.
 * 
 * The function returns null if the data file for the block doesn't exist.
 * The returned value should be freed by manifest_free_tuple.
 */
ManifestTuple manifest_find(ManifestRelation mrel, Snapshot snapshot, int block);

/**
 * Release ManifestTuple acquired from manifest_find().
 */
void manifest_free_tuple(ManifestTuple tuple);

/**
 * Swap data between two pax tables, but not swap oids. The exchanged files
 * contain data files, visimap files, toast files, fast sequence numbers,
 * manifest contents.
 * 
 * See the usage on swap function in TableAmRoutine.
 */
void manifest_swap_table(Oid relid1, Oid relid2,
                         TransactionId frozen_xid,
                         MultiXactId cutoff_multi);
#ifdef __cplusplus
}
#endif
