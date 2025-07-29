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

#include <Python.h>
#include <structmember.h>

#include <string>

#include "comm/fmt.h"
#include "exceptions/CException.h"
#include "paxpy_types.h"
#include "storage/file_system.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition_file_factory.h"
#include "storage/orc/orc_dump_reader.h"
#include "storage/orc/porc.h"

int paxpy_debug_enabled = 1;
#define INIT_MODULE(m) PyInit_##m

static struct PyModuleDef PAXpymodule = {
    PyModuleDef_HEAD_INIT, "paxpy", NULL, -1, NULL, NULL, NULL, NULL, NULL};

static int add_module_types(PyObject *module) {
  int i;

  PAXPY_PRINT("psycopgmodule: initializing module types");

  for (i = 0; typetable[i].name; i++) {
    PyObject *type = (PyObject *)typetable[i].type;

    Py_SET_TYPE(typetable[i].type, &PyType_Type);
    if (0 > PyType_Ready(typetable[i].type)) {
      return -1;
    }

    Py_INCREF(type);
    if (0 > PyModule_AddObject(module, typetable[i].name, type)) {
      Py_DECREF(type);
      return -1;
    }
  }
  return 0;
}

#ifndef PyMODINIT_FUNC /* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC INIT_MODULE(paxpy)(void) {
  PyObject *module = PyModule_Create(&PAXpymodule);
  if (!module) {
    goto error;
  }
  if (datetime_types_init() < 0) {
    goto error;
  }
  if (add_module_types(module) < 0) {
    goto error;
  }

  return module;

error:
  if (module) Py_DECREF(module);
  return NULL;
}
