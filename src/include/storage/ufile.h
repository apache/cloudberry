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
 * ufile.h
 *	  Unified file abstraction and manipulation.
 *
 * src/include/storage/ufile.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef UFILE_H
#define UFILE_H

#include "storage/relfilelocator.h"

#define UFILE_ERROR_SIZE	1024

struct UFile;

typedef struct FileAm
{
	struct UFile* (*open) (Oid spcId, const char *fileName, int fileFlags,
				char *errorMessage, int errorMessageSize);
	int (*close) (struct UFile *file);
	int (*sync) (struct UFile *file);
	int (*read) (struct UFile *file, char *buffer, int amount);
	int (*write) (struct UFile *file, char *buffer, int amount);
	int64_t (*size) (struct UFile *file);
	void (*unlink) (Oid spcId, const char *fileName);
	char* (*formatPathName) (RelFileLocator *relFileLocator);
	bool (*ensurePath) (Oid spcId, const char *pathName);
	bool (*exists) (Oid spcId, const char *fileName);
	const char *(*name) (struct UFile *file);
	const char *(*getLastError) (void);
	void (*getConnection) (Oid spcId);
} FileAm;

typedef struct UFile
{
	FileAm *methods;
} UFile;

extern UFile *UFileOpen(Oid spcId,
						const char *fileName,
						int fileFlags,
						char *errorMessage,
						int errorMessageSize);
extern int UFileClose(UFile *file);
extern int UFileSync(UFile *fiLe);

extern int UFileRead(UFile *file, char *buffer, int amount);
extern int UFileWrite(UFile *file, char *buffer, int amount);

extern off_t UFileSize(UFile *file);
extern const char *UFileName(UFile *file);

extern void UFileUnlink(Oid spcId, const char *fileName);
extern char* UFileFormatPathName(RelFileLocator *relFileNode);
extern bool UFileEnsurePath(Oid spcId, const char *pathName);
extern bool UFileExists(Oid spcId, const char *fileName);

extern const char *UFileGetLastError(UFile *file);
extern void forceCacheUFileResource(Oid id);

extern struct FileAm localFileAm;

#endif //UFILE_H
