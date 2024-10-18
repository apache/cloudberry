/*-------------------------------------------------------------------------
 *
 * Storage manipulation for directory table.
 *
 * Copyright (c) 2016-Present Hashdata, Inc.
 *
 * src/include/catalog/storage_directory_table.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef STORAGE_DIRECTORY_TABLE_H
#define STORAGE_DIRECTORY_TABLE_H

#include "utils/relcache.h"

extern bool allow_dml_directory_table;

extern void UFileAddPendingDelete(Relation rel, Oid spcId, char *relativePath, bool atCommit);
extern void DirectoryTableDropStorage(Oid relid);

#endif	/* STORAGE_DIRECTORY_TABLE_H */
