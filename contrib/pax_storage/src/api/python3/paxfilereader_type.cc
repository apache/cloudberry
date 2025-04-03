#include "paxpy_types.h"

#include "comm/fmt.h"
#include "comm/singleton.h"
#include "exceptions/CException.h"
#include "storage/file_system.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition_file_factory.h"
#include "storage/orc/porc.h"
#include "storage/toast/pax_toast.h"

static struct PyMemberDef paxfilereader_members[] = {
    {"schema", T_OBJECT, offsetof(PaxFileReaderObject, schema), 0}, {NULL}};

static void paxfilereader_dealloc(PyObject *obj) {
  PaxFileReaderObject *self = (PaxFileReaderObject *)obj;

  // can't clear the schema
  // once exception happen, it will be cleared by gc
  // Py_CLEAR(self->schema);
  delete self->reader;

  PAXPY_PRINT(
      "paxfilereader_dealloc: deleted paxfile reader object at %p, refcnt "
      "= " FORMAT_CODE_PY_SSIZE_T,
      obj, Py_REFCNT(obj));
  Py_TYPE(obj)->tp_free(obj);
}

static PyObject *paxfilereader_repr(PaxFileReaderObject *self) {
  return PyUnicode_FromFormat("<paxfilereader object at %p;>", self);
}

static int paxfilereader_traverse(PaxFileObject *self, visitproc visit,
                                  void *arg) {
  return 0;
}

static int paxfilereader_clear(PaxFileObject *self) { return 0; }

static bool check_schema(PyObject *schema) {
  if (!schema || !PyList_Check(schema)) {
    PyErr_SetString(PyExc_TypeError, "schema must be a list[long]");
    return false;
  }

  if (PyList_Size(schema) < 0) {
    PyErr_SetString(PyExc_TypeError,
                    pax::fmt("the size of schema list is invalid [size=%ld]",
                             PyList_Size(schema))
                        .c_str());
    return false;
  }

  // For the memory safe, Check the schema before we use it.
  for (Py_ssize_t i = 0; i < PyList_Size(schema); i++) {
    auto schema_item = PyList_GET_ITEM(schema, i);
    if (schema_item == Py_None || !PyLong_Check(schema_item)) {
      PyErr_SetString(
          PyExc_TypeError,
          "Exist None or invalid type in schema, schema list must be "
          "list[long] without any None \n");
      return false;
    }
  }

  return true;
}

static bool check_projection(PyObject *proj, Py_ssize_t schema_len) {
  // allow no projection here
  if (!proj || proj == Py_None) {
    return true;
  }

  if (!PyList_Check(proj)) {
    PyErr_SetString(PyExc_TypeError, "proj must be a list[bool]");
    return false;
  }

  if (PyList_Size(proj) != schema_len) {
    PyErr_SetString(PyExc_TypeError,
                    pax::fmt("the size of projection list must equal with "
                             "schema [size=%ld, schema_len=%ld]",
                             PyList_Size(proj), schema_len)
                        .c_str());
    return false;
  }

  // For the memory safe, Check the schema before we use it.
  for (Py_ssize_t i = 0; i < PyList_Size(proj); i++) {
    auto proj_item = PyList_GET_ITEM(proj, i);
    if (proj_item == Py_None || !PyBool_Check(proj_item)) {
      PyErr_SetString(
          PyExc_TypeError,
          "Exist None or invalid type in projection, projection list must be "
          "list[bool] without any None \n");
      return false;
    }
  }

  return true;
}

static int paxfilereader_init(PyObject *self, PyObject *args,
                              PyObject *kwargs) {
  PyObject *schema = NULL, *proj = NULL, *pax_file = NULL;
  PaxFileObject *pax_file_obj;
  std::shared_ptr<pax::Bitmap8> visible_map_bm = nullptr;
  std::shared_ptr<pax::File> toast_file = nullptr;

  PaxFileReaderObject *pax_file_reader;
  pax_file_reader = (PaxFileReaderObject *)self;

  if (!(pax_file_reader->schema = PyList_New(0))) {
    return -1;
  }

  static char *kwlist[] = {"schema", "projection", "paxfile", NULL};
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OOO", kwlist, &schema, &proj,
                                   &pax_file)) {
    return -1;
  }

  if (!check_schema(schema) || !check_projection(proj, PyList_Size(schema))) {
    return -1;
  }

  Py_INCREF(pax_file);  // hold the memory

  if (!pax_file || !PyObject_TypeCheck(pax_file, &paxfileType)) {
    PyErr_SetString(PyExc_TypeError,
                    "need create a PaxFileObject to access file");
    goto get_error;
  }

  // init the pg error memory context
  ErrorContext = AllocSetContextCreate((MemoryContext) nullptr, "ErrorContext",
                                       8 * 1024, 8 * 1024, 8 * 1024);
  MemoryContextAllowInCriticalSection(ErrorContext, true);

  pax_file_obj = (PaxFileObject *)pax_file;
  PAXPY_PRINT(
      "Begin to create the reader [filepath: %s, vmfilepath: %s, "
      "toastfilepath: %s] \n",
      pax_file_obj->filepath, pax_file_obj->vmfilepath,
      pax_file_obj->toastfilepath);

  if (pax_file_obj->vmfilepath) {
    try {
      auto vmfile_ptr =
          pax::Singleton<pax::LocalFileSystem>::GetInstance()->Open(
              pax_file_obj->vmfilepath, pax::fs::kReadMode);
      auto vmfile_len = vmfile_ptr->FileLength();
      visible_map_bm = std::make_shared<pax::Bitmap8>(vmfile_len * 8);
      vmfile_ptr->ReadN(visible_map_bm->Raw().bitmap, vmfile_len);
      vmfile_ptr->Close();
    } catch (cbdb::CException &e) {
      PyErr_SetString(
          PyExc_TypeError,
          pax::fmt(
              "Fail to open/read the visible file[vmfilepath: %s, error=%s] \n",
              pax_file_obj->vmfilepath, e.What().c_str())
              .c_str());
      goto get_error;
    }
  }

  if (pax_file_obj->toastfilepath) {
    try {
      toast_file = pax::Singleton<pax::LocalFileSystem>::GetInstance()->Open(
          pax_file_obj->toastfilepath, pax::fs::kReadMode);
    } catch (cbdb::CException &e) {
      PyErr_SetString(
          PyExc_TypeError,
          pax::fmt(
              "Fail to open the toast file[toastfilepath: %s, error=%s] \n",
              pax_file_obj->toastfilepath, e.What().c_str())
              .c_str());
      goto get_error;
    }
  }

  try {
    std::shared_ptr<pax::PaxFilter> proj_filter = nullptr;

    if (proj && proj != Py_None) {
      std::vector<bool> proj_list;

      for (Py_ssize_t i = 0; i < PyList_Size(proj); i++) {
        auto proj_item = PyList_GET_ITEM(proj, i);
        assert(proj_item == Py_True || proj_item == Py_False);
        proj_list.emplace_back(proj_item == Py_True ? true : false);
      }

      proj_filter = std::make_shared<pax::PaxFilter>();
      proj_filter->SetColumnProjection(std::move(proj_list));
    }

    pax::MicroPartitionReader::ReaderOptions read_options{
        .filter = proj_filter,
        // TODO(jiaqizho): use the reused buffer
        .visibility_bitmap = visible_map_bm,
    };

    auto file_ptr = pax::Singleton<pax::LocalFileSystem>::GetInstance()->Open(
        pax_file_obj->filepath, pax::fs::kReadMode);
    auto reader = new pax::OrcReader(std::move(file_ptr), toast_file);
    reader->Open(std::move(read_options));
    pax_file_reader->reader = reader;
  } catch (cbdb::CException &e) {
    PyErr_SetString(PyExc_TypeError,
                    pax::fmt("Fail to create the reader [filepath: %s, "
                             "vmfilepath: %s, toastfilepath: %s, error=%s] \n",
                             pax_file_obj->filepath, pax_file_obj->vmfilepath,
                             pax_file_obj->toastfilepath, e.What().c_str())
                        .c_str());
    goto get_error;
  }

  // Already checked the schema is legal
  for (Py_ssize_t i = 0; i < PyList_Size(schema); i++) {
    auto schema_item = PyList_GET_ITEM(schema, i);
    PyList_Append(pax_file_reader->schema, schema_item);
  }

  PAXPY_PRINT(
      "Succ create the reader [filepath: %s, vmfilepath: %s, toastfilepath: "
      "%s] \n",
      pax_file_obj->filepath, pax_file_obj->vmfilepath,
      pax_file_obj->toastfilepath);

  MemoryContextReset(ErrorContext);
  Py_DECREF(pax_file);
  return 0;
get_error:
  MemoryContextReset(ErrorContext);
  Py_DECREF(pax_file);
  return -1;
}

static PyObject *paxfilereader_new(PyTypeObject *type, PyObject *args,
                                   PyObject *kwds) {
  return type->tp_alloc(type, 0);
}

// self defined method

static PyObject *paxfilereader_close(PaxFileReaderObject *self,
                                     PyObject *dummy) {
  assert(self->reader);
  self->reader->Close();
  Py_RETURN_NONE;
}

static PyObject *paxfilereader_getgroupnums(PaxFileReaderObject *self,
                                            PyObject *dummy) {
  assert(self->reader);
  auto gn = self->reader->GetGroupNums();
  return PyLong_FromSize_t(gn);
}

static PyObject *paxfilereader_readgroup(PaxFileReaderObject *self,
                                         PyObject *args) {
  unsigned int index = 0;
  size_t col_nums;
  size_t row_nums;
  PyObject *py_cols = NULL, *py_rows = NULL;
  bool is_vec = false;
  std::unique_ptr<pax::MicroPartitionReader::Group> group = nullptr;
  std::shared_ptr<pax::PaxColumns> columns = nullptr;
  std::shared_ptr<pax::Bitmap8> visible_map;
  size_t row_offset;
  size_t column_index = 0;

  if (!PyArg_ParseTuple(args, "I", &index)) return nullptr;

  if (index >= self->reader->GetGroupNums()) {
    PyErr_SetString(PyExc_TypeError,
                    pax::fmt("current [index=%u] out of group range[0, %lu].",
                             index, self->reader->GetGroupNums())
                        .c_str());
    return nullptr;
  }

  if (!(py_cols = PyList_New(0))) {
    return nullptr;
  }

  ErrorContext = AllocSetContextCreate((MemoryContext) nullptr, "ErrorContext",
                                       8 * 1024, 8 * 1024, 8 * 1024);
  MemoryContextAllowInCriticalSection(ErrorContext, true);

  try {
    group = self->reader->ReadGroup(index);
    visible_map = group->GetVisibilityMap();
    columns = group->GetAllColumns();
    row_offset = group->GetRowOffset();

    row_nums = columns->GetRows();
    col_nums = columns->GetColumns();

    is_vec = columns->GetStorageFormat() ==
             pax::PaxStorageFormat::kTypeStoragePorcVec;
  } catch (cbdb::CException &e) {
    PyErr_SetString(PyExc_TypeError,
                    pax::fmt("Failed to open current group[index=%d,error=%s]",
                             index, e.What().c_str())
                        .c_str());
    goto get_error;
  }

  if (col_nums > PyList_Size(self->schema)) {
    PyErr_SetString(
        PyExc_TypeError,
        pax::fmt(
            "current group is invalid. [schema size=%ld, columns in group=%ld]",
            PyList_Size(self->schema), col_nums)
            .c_str());
    goto get_error;
  }

  try {
    for (; column_index < col_nums; column_index++) {
      const auto &column = (*columns)[column_index];
      std::shared_ptr<pax::Bitmap8> bm;
      auto null_counts = 0;
      PyObject *schema_item = nullptr;
      long col_oid;

      char *buff;
      size_t buff_len;

      // filter by projection
      if (!column) {
        Py_INCREF(Py_None);
        PyList_Append(py_cols, Py_None);
        Py_DECREF(Py_None);
        continue;
      }

      schema_item = PyList_GET_ITEM(self->schema, column_index);
      // safe to direct cast to long,already checked schema in init
      col_oid = PyLong_AsLong(schema_item);

      // dropped column
      if (col_oid == InvalidOid) {
        Py_INCREF(Py_None);
        PyList_Append(py_cols, Py_None);
        Py_DECREF(Py_None);
        continue;
      }

      bm = column->GetBitmap();

      py_rows = PyList_New(0);
      if (!py_rows) {
        goto get_error;
      }

      for (size_t row_index = 0; row_index < row_nums; row_index++) {
        // skip the invisible rows
        if (visible_map && visible_map->Test(row_offset + row_index)) {
          if (bm && !bm->Test(row_index)) {
            null_counts++;
          }
          continue;
        }

        if (bm && !bm->Test(row_index)) {
          Py_INCREF(Py_None);
          PyList_Append(py_rows, Py_None);
          Py_DECREF(Py_None);
          null_counts++;
        } else {
          PyObject *datum_pyobj;

          std::tie(buff, buff_len) =
              column->GetBuffer(is_vec ? row_index : row_index - null_counts);

          if (column->IsToast(row_index)) {
            // safe to no keep the ref, because paxbuffer_to_pyobj will copy the
            // detoast datum
            std::shared_ptr<pax::MemoryObject> ref = nullptr;

            auto datum = PointerGetDatum(buff);
            auto external_buffer = column->GetExternalToastDataBuffer();
            std::tie(datum, ref) = pax::pax_detoast(
                datum, external_buffer ? external_buffer->Start() : nullptr,
                external_buffer ? external_buffer->Used() : 0);

            datum_pyobj = paxbuffer_to_pyobj(col_oid, DatumGetPointer(datum),
                                             VARSIZE_ANY(datum), false);
          } else {
            datum_pyobj = paxbuffer_to_pyobj(col_oid, buff, buff_len, is_vec);
          }

          if (datum_pyobj == nullptr) {
            PyErr_SetString(PyExc_TypeError,
                            pax::fmt("Fail to parse current row[index=%d, "
                                     "col_idx=%ld, row_idx=%ld]",
                                     index, column_index, row_index)
                                .c_str());
            goto get_error;
          }

          // move the datum_pyobj into the py_rows
          PyList_Append(py_rows, datum_pyobj);
          Py_DECREF(datum_pyobj);
        }
      }

      // move current py_rows into the py_cols
      PyList_Append(py_cols, py_rows);
      Py_DECREF(py_rows);
    }
  } catch (cbdb::CException &e) {
    PyErr_SetString(PyExc_TypeError,
                    pax::fmt("Fail to read current group[index=%d, error=%s]",
                             index, e.What().c_str())
                        .c_str());
    goto get_error;
  }

  // missing the add column
  for (; column_index < PyList_Size(self->schema); column_index++) {
    Py_INCREF(Py_None);
    PyList_Append(py_cols, Py_None);
    Py_DECREF(Py_None);
  }

  return py_cols;
get_error:
  Py_XDECREF(py_cols);
  return nullptr;
}

static struct PyMethodDef paxfilereader_methods[] = {
    {"close", (PyCFunction)paxfilereader_close, METH_VARARGS | METH_KEYWORDS,
     "Close current pax filer reader"},
    {"getgroupnums", (PyCFunction)paxfilereader_getgroupnums,
     METH_VARARGS | METH_KEYWORDS, "Get the number of groups"},
    {"readgroup", (PyCFunction)paxfilereader_readgroup,
     METH_VARARGS | METH_KEYWORDS, "Open a group for read"},
    {nullptr}};

PyTypeObject paxfilereader_type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "paxpy.paxfilereader",
    .tp_basicsize = sizeof(PaxFileReaderObject),
    .tp_itemsize = 0,
    .tp_dealloc = paxfilereader_dealloc,
#if PY_VERSION_HEX >= 0x03090000
    .tp_vectorcall_offset = 0,
#else
    .tp_print = 0,
#endif
    .tp_getattr = 0,
    .tp_setattr = 0,
    .tp_repr = (reprfunc)paxfilereader_repr,
    .tp_as_number = 0,
    .tp_as_sequence = 0,
    .tp_as_mapping = 0,
    .tp_hash = 0,
    .tp_call = 0,
    .tp_str = (reprfunc)paxfilereader_repr,
    .tp_getattro = 0,
    .tp_setattro = 0,
    .tp_as_buffer = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_doc = "The pax file reader",
    .tp_traverse = (traverseproc)paxfilereader_traverse,
    .tp_clear = (inquiry)paxfilereader_clear,
    .tp_richcompare = 0,
    .tp_iter = 0,
    .tp_iternext = 0,
    .tp_methods = paxfilereader_methods,
    .tp_members = paxfilereader_members,
    .tp_getset = 0,
    .tp_base = 0,
    .tp_dict = 0,
    .tp_descr_get = 0,
    .tp_descr_set = 0,
    .tp_dictoffset = 0,
    .tp_init = paxfilereader_init,
    .tp_alloc = 0,
    .tp_new = paxfilereader_new,
};
