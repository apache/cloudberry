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
 *-------------------------------------------------------------------------
 */

#pragma once
#include <Python.h>
#include <datetime.h>
#include <structmember.h>

#include "paxpy_comm.h"
#include "storage/micro_partition.h"

// comm defined
#define Py_TPFLAGS_HAVE_ITER 0L
#define Py_TPFLAGS_HAVE_RICHCOMPARE 0L
#define Py_TPFLAGS_HAVE_WEAKREFS 0L

#if PY_VERSION_HEX < 0x030900A4
#define Py_SET_TYPE(obj, type) ((Py_TYPE(obj) = (type)), (void)0)
#endif

int datetime_types_init(void);
PyObject *paxbuffer_to_pyobj(Oid oid, char *buff, size_t len, bool is_vec);

struct PaxFileObject {
  PyObject_HEAD;

  char *filepath = nullptr;       // require, the pax file path
  char *vmfilepath = nullptr;     // options, the visible map file path
  char *toastfilepath = nullptr;  // options, the toast file path
};
typedef struct PaxFileObject PaxFileObject;
extern PyTypeObject paxfileType;

struct PaxFileReaderObject {
  PyObject_HEAD;

  PyObject *schema = nullptr;                   // the oids
  pax::MicroPartitionReader *reader  = nullptr;  // don't use the smart pointer
};
typedef struct PaxFileReaderObject PaxFileReaderObject;
extern PyTypeObject paxfilereader_type;

struct {
  char *name;
  PyTypeObject *type;
} typetable[] = {
    {"paxfile", &paxfileType},
    {"paxfilereader", &paxfilereader_type},
    {NULL} /* Sentinel */
};
