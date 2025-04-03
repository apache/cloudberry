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
