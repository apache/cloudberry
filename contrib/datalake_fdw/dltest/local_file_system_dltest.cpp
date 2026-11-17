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
 * local_file_system_dltest.cpp
 *	  The create-only local file system, compiled into this library.
 *
 * The same source the extension uses, built here without the part that
 * registers it, so that this library has no symbol it expects datalake_fdw
 * to supply.  Including it from a file of its own name, rather than compiling
 * the other file under a renamed target, lets the ordinary rules build both
 * the object and, on a server configured --with-llvm, its bitcode.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/dltest/local_file_system_dltest.cpp
 *
 *-------------------------------------------------------------------------
 */

#define DL_LOCAL_FS_NO_REGISTER

#include "common/local_file_system.cpp"
