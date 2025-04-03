#include "paxpy_types.h"

static struct PyMemberDef paxfile_members[] = {
    {"filepath", T_STRING, offsetof(PaxFileObject, filepath), READONLY,
     "The pax file path"},
    {"vmfilepath", T_STRING, offsetof(PaxFileObject, vmfilepath), READONLY,
     "The visible map file path, need pass it if exist."},
    {"toastfilepath", T_STRING, offsetof(PaxFileObject, toastfilepath),
     READONLY, "The toast file path, need pass it if exist."},
    {NULL}};

static void paxfile_dealloc(PyObject *obj) {
  PaxFileObject *self = (PaxFileObject *)obj;

  free(self->filepath);
  if (self->vmfilepath) free(self->vmfilepath);
  if (self->toastfilepath) free(self->toastfilepath);

  PAXPY_PRINT(
      "paxfile_dealloc: deleted paxfile object at %p, refcnt "
      "= " FORMAT_CODE_PY_SSIZE_T,
      obj, Py_REFCNT(obj));
  Py_TYPE(obj)->tp_free(obj);
}

static PyObject *paxfile_repr(PaxFileObject *self) {
  return PyUnicode_FromFormat(
      "<paxfile object at %p; filepath: '%s', vmfilepath: %s, toastfilepath: "
      "%s>",
      self, self->filepath, self->vmfilepath ? self->vmfilepath : "None",
      self->toastfilepath ? self->toastfilepath : "None");
}

static int paxfile_traverse(PaxFileObject *self, visitproc visit, void *arg) {
  return 0;
}

static int paxfile_clear(PaxFileObject *self) { return 0; }

static int paxfile_init(PyObject *obj, PyObject *args, PyObject *kwds) {
  PaxFileObject *pax_file;
  char *filepath = nullptr, *vmfilepath = nullptr, *toastfilepath = nullptr;
  int rc = 0;
  static char *kwlist[] = {"filepath", "vmfilepath", "toastfilepath", NULL};

  if (!PyArg_ParseTupleAndKeywords(args, kwds, "s|ss", kwlist, &filepath,
                                   &vmfilepath, &toastfilepath))
    return -1;
  pax_file = (PaxFileObject *)obj;

  auto copy_py_str = [](const char *src, char **dst) -> int {
    if (!src) {
      return -1;
    }

    *dst = (char *)malloc(strlen(src) + 1);

    if (!*dst) {
      return -1;
    }
    strcpy(*dst, src);
    return 0;
  };

  rc |= copy_py_str(filepath, &pax_file->filepath);
  if (vmfilepath) rc |= copy_py_str(vmfilepath, &pax_file->vmfilepath);
  if (toastfilepath) rc |= copy_py_str(toastfilepath, &pax_file->toastfilepath);
  return rc;
}

static PyObject *paxfile_new(PyTypeObject *type, PyObject *args,
                             PyObject *kwds) {
  return type->tp_alloc(type, 0);
}

PyTypeObject paxfileType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "paxpy.paxfile",
    .tp_basicsize = sizeof(PaxFileObject),
    .tp_itemsize = 0,
    .tp_dealloc = paxfile_dealloc,
#if PY_VERSION_HEX >= 0x03090000
    .tp_vectorcall_offset = 0,
#else
    .tp_print = 0,
#endif
    .tp_getattr = 0,
    .tp_setattr = 0,
    .tp_repr = (reprfunc)paxfile_repr,
    .tp_as_number = 0,
    .tp_as_sequence = 0,
    .tp_as_mapping = 0,
    .tp_hash = 0,
    .tp_call = 0,
    .tp_str = (reprfunc)paxfile_repr,
    .tp_getattro = 0,
    .tp_setattro = 0,
    .tp_as_buffer = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_doc = "paxfile(filepath, ...) -> new a pax file object\n",
    .tp_traverse = (traverseproc)paxfile_traverse,
    .tp_clear = (inquiry)paxfile_clear,
    .tp_richcompare = 0,
    .tp_iter = 0,
    .tp_iternext = 0,
    .tp_methods = 0,
    .tp_members = paxfile_members,
    .tp_getset = 0,
    .tp_base = 0,
    .tp_dict = 0,
    .tp_descr_get = 0,
    .tp_descr_set = 0,
    .tp_dictoffset = 0,
    .tp_init = paxfile_init,
    .tp_alloc = 0,
    .tp_new = paxfile_new,
    .tp_free = PyObject_Del,
};
