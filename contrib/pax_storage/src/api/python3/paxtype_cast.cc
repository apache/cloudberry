#include "paxpy_types.h"

#include "comm/fmt.h"
#include "comm/singleton.h"
#include "exceptions/CException.h"
#include "storage/orc/porc.h"
#include "storage/toast/pax_toast.h"
#include "utils/date.h"

int datetime_types_init(void) {
  PyDateTime_IMPORT;

  if (!PyDateTimeAPI) {
    PyErr_SetString(PyExc_ImportError, "datetime initialization failed");
    return -1;
  }
  return 0;
}

void parse_pg_date(int jd, int *year, int *month, int *day) {
  unsigned int julian;
  unsigned int quad;
  unsigned int extra;
  int y;

  julian = jd;
  julian += 32044;
  quad = julian / 146097;
  extra = (julian - quad * 146097) * 4 + 3;
  julian += 60 + quad * 3 + extra / 146097;
  quad = julian / 1461;
  julian -= quad * 1461;
  y = julian * 4 / 1461;
  julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366)) + 123;
  y += quad * 4;
  *year = y - 4800;
  quad = julian * 2141 / 65536;
  *day = julian - 7834 * quad / 256;
  *month = (quad + 10) % MONTHS_PER_YEAR + 1;
}

static bool pg_array_get_isnull(const bits8 *nullbitmap, int offset) {
  if (nullbitmap == NULL) return false; /* assume not null */
  if (nullbitmap[offset / 8] & (1 << (offset % 8))) return false; /* not null */
  return true;
}

static PyObject *pg_array_to_py_array(
    char *buff, size_t len, int typlen, char typalign,
    PyObject *(*datum_to_pyobj)(Datum datum)) {
  PyObject *py_array = NULL;
  bits8 *nullbitmap;
  int nitems;
  char *data_ptr;

  // no toast exist in pax memory
  // memory safe to call
  auto arr = cbdb::DatumToArrayTypeP(cbdb::PointerToDatum(buff));

  if (!(py_array = PyList_New(0))) {
    return nullptr;
  }

  nullbitmap = ARR_NULLBITMAP(arr);  // memory safe to call
  nitems = cbdb::ArrayGetN(ARR_NDIM(arr), ARR_DIMS(arr));
  data_ptr = ARR_DATA_PTR(arr);  // memory safe to call

  for (int i = 0; i < nitems; i++) {
    if (pg_array_get_isnull(nullbitmap, i)) {
      Py_INCREF(Py_None);
      PyList_Append(py_array, Py_None);
      Py_DECREF(Py_None);
    } else {
      char *p = data_ptr;
      auto d = fetch_att(p, true, typlen);  // memory safe to call
      auto entry_pyobj = datum_to_pyobj(d);

      if (entry_pyobj == nullptr) {
        Py_DECREF(py_array);
        return nullptr;
      }
      PyList_Append(py_array, entry_pyobj);

      p = att_addlength_pointer(p, typlen, p);     // memory safe to call
      p = (char *)att_align_nominal(p, typalign);  // memory safe to call
      data_ptr = p;
    }
  }

  return py_array;
}

static std::shared_ptr<pax::ByteBuffer> pax_vec_buff_add_header(char *buffer,
                                                                size_t len) {
  auto buff_with_header_len = TYPEALIGN(MEMORY_ALIGN_SIZE, len + VARHDRSZ);
  auto buff_ref = std::make_shared<pax::ByteBuffer>(buff_with_header_len,
                                                    buff_with_header_len);
  auto buff_with_header = buff_ref->Addr();

  SET_VARSIZE(buff_with_header, len + VARHDRSZ);
  memcpy(VARDATA(buff_with_header), buffer, len);
  return buff_ref;
}

PyObject *paxbuffer_to_pyobj(Oid oid, char *buff, size_t len, bool is_vec) {
  switch (oid) {
    case INT2OID: {
      if (len != sizeof(int16_t)) {
        return nullptr;
      }
      auto i16 = (int16_t *)buff;
      return PyLong_FromLong((long)*i16);
    }
    case INT4OID: {
      if (len != sizeof(int32_t)) {
        return nullptr;
      }
      auto i32 = (int32_t *)buff;
      return PyLong_FromLong(*i32);
    }
    case INT8OID: {
      if (len != sizeof(int64_t)) {
        return nullptr;
      }
      auto i64 = (int64_t *)buff;
      return PyLong_FromLongLong(*i64);
    }
    case FLOAT4OID: {
      if (len != sizeof(float)) {
        return nullptr;
      }
      auto f32 = (float *)buff;
      return PyFloat_FromDouble((double)*f32);
    }
    case FLOAT8OID: {
      if (len != sizeof(double)) {
        return nullptr;
      }
      auto f64 = (double *)buff;
      return PyFloat_FromDouble(*f64);
    }
    case BYTEAOID: {
      if (is_vec) {
        return PyBytes_FromStringAndSize(buff, len);
      } else {
        auto val = (struct varlena *)buff;
        return PyBytes_FromStringAndSize(VARDATA_ANY(val),
                                         VARSIZE_ANY_EXHDR(val));
      }
    }
    case TEXTOID:
    case VARCHAROID:  // no support bpchar
    {
      if (is_vec) {
        return PyUnicode_FromStringAndSize(buff, len);
      } else {
        auto val = (struct varlena *)buff;
        // won't be toast here
        return PyUnicode_FromStringAndSize(VARDATA_ANY(val),
                                           VARSIZE_ANY_EXHDR(val));
      }
    }
    case BOOLOID: {
      PyObject *obj = nullptr;
      if (len != sizeof(bool)) {
        return nullptr;
      }
      obj = (*(bool *)buff) ? Py_True : Py_False;
      Py_XINCREF(obj);
      return obj;
    }
    case DATEOID: {
      PyObject *obj = NULL;
      auto date32 = (DateADT *)buff;
      int y = 0, m = 0, d = 0;

      if (len != sizeof(DateADT)) {
        return nullptr;
      }

      if (DATE_IS_NOBEGIN(*date32)) {
        obj =
            PyObject_GetAttrString((PyObject *)PyDateTimeAPI->DateType, "min");
      } else if (DATE_IS_NOEND(*date32)) {
        obj =
            PyObject_GetAttrString((PyObject *)PyDateTimeAPI->DateType, "max");
      } else {
        // memory safe to call
        parse_pg_date(*date32 + POSTGRES_EPOCH_JDATE, &y, &m, &d);
        obj = PyObject_CallFunction((PyObject *)PyDateTimeAPI->DateType, "iii",
                                    y, m, d);
      }
      return obj;
    }
    case TIMEOID: {
      PyObject *obj = NULL;
      PyObject *tzinfo = NULL;
      auto time64 = (TimeADT *)buff;
      fsec_t fsec;
      struct pg_tm tt, *tm = &tt;

      if (len != sizeof(TimeADT)) {
        return nullptr;
      }

      tzinfo = Py_None;
      Py_INCREF(tzinfo);

      // memory safe to call
      time2tm(*time64, tm, &fsec);
      obj = PyObject_CallFunction((PyObject *)PyDateTimeAPI->TimeType, "iiiiO",
                                  tm->tm_hour, tm->tm_min, tm->tm_sec, fsec,
                                  tzinfo);
      return obj;
    }
    case TIMESTAMPOID:
    case TIMESTAMPTZOID: {
      PyObject *obj = NULL;
      PyObject *tzinfo = NULL;
      Timestamp *ts64;

      if (len != sizeof(Timestamp)) {
        return nullptr;
      }
      ts64 = (Timestamp *)buff;
      if (TIMESTAMP_IS_NOBEGIN(*ts64)) {
        obj = PyObject_GetAttrString((PyObject *)PyDateTimeAPI->DateTimeType,
                                     "min");
      } else if (TIMESTAMP_IS_NOEND(*ts64)) {
        obj = PyObject_GetAttrString((PyObject *)PyDateTimeAPI->DateTimeType,
                                     "max");
      } else {
        struct pg_tm tt, *tm = &tt;
        fsec_t fsec;

        // memory safe to call
        if (timestamp2tm(*ts64, NULL, tm, &fsec, NULL, NULL) != 0) {
          return nullptr;
        }

        tzinfo = Py_None;
        Py_INCREF(tzinfo);

        obj = PyObject_CallFunction(
            (PyObject *)PyDateTimeAPI->DateTimeType, "iiiiiiiO",
            (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1), tm->tm_mon,
            tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, fsec, tzinfo);
      }
      return obj;
    }
    case INT2ARRAYOID: {
      auto datum_to_py_obj = [](Datum d) {
        int16_t i16 = cbdb::DatumToInt16(d);
        return PyLong_FromLong((long)i16);
      };
      if (is_vec) {
        auto ref = pax_vec_buff_add_header(buff, len);
        return pg_array_to_py_array((char *)ref->Addr(), ref->Size(), 2,
                                    TYPALIGN_SHORT, datum_to_py_obj);
      } else {
        return pg_array_to_py_array(buff, len, 2, TYPALIGN_SHORT,
                                    datum_to_py_obj);
      }
    }
    case INT4ARRAYOID: {
      auto datum_to_py_obj = [](Datum d) {
        int32_t i32 = cbdb::DatumToInt32(d);
        return PyLong_FromLong(i32);
      };

      if (is_vec) {
        auto ref = pax_vec_buff_add_header(buff, len);
        return pg_array_to_py_array((char *)ref->Addr(), ref->Size(), 4,
                                    TYPALIGN_INT, datum_to_py_obj);
      } else {
        return pg_array_to_py_array(buff, len, 4, TYPALIGN_INT,
                                    datum_to_py_obj);
      }
    }
    case INT8ARRAYOID: {
      auto datum_to_py_obj = [](Datum d) {
        int64_t i64 = cbdb::DatumToInt64(d);
        return PyLong_FromLongLong(i64);
      };

      if (is_vec) {
        auto ref = pax_vec_buff_add_header(buff, len);
        return pg_array_to_py_array((char *)ref->Addr(), ref->Size(), 8,
                                    TYPALIGN_DOUBLE, datum_to_py_obj);
      } else {
        return pg_array_to_py_array(buff, len, 8, TYPALIGN_DOUBLE,
                                    datum_to_py_obj);
      }
    }
    case FLOAT4ARRAYOID: {
      auto datum_to_py_obj = [](Datum d) {
        float f32 = cbdb::DatumToFloat4(d);
        return PyFloat_FromDouble((double)f32);
      };

      if (is_vec) {
        auto ref = pax_vec_buff_add_header(buff, len);
        return pg_array_to_py_array((char *)ref->Addr(), ref->Size(), 4,
                                    TYPALIGN_INT, datum_to_py_obj);
      } else {
        return pg_array_to_py_array(buff, len, 4, TYPALIGN_INT,
                                    datum_to_py_obj);
      }
    }
    case FLOAT8ARRAYOID: {
      auto datum_to_py_obj = [](Datum d) {
        double f64 = cbdb::DatumToFloat8(d);
        return PyFloat_FromDouble(f64);
      };

      if (is_vec) {
        auto ref = pax_vec_buff_add_header(buff, len);
        return pg_array_to_py_array((char *)ref->Addr(), ref->Size(), 8,
                                    TYPALIGN_DOUBLE, datum_to_py_obj);
      } else {
        return pg_array_to_py_array(buff, len, 8, TYPALIGN_DOUBLE,
                                    datum_to_py_obj);
      }
    }
    default:
      break;
  }

  if (is_vec) {
      return PyBytes_FromStringAndSize(buff, len);
  } else {
    auto val = (struct varlena *)buff;
    return PyBytes_FromStringAndSize(VARDATA_ANY(val),
                                    VARSIZE_ANY_EXHDR(val));
  }
}
