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
 * CException.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/exceptions/CException.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "comm/cbdb_api.h"

#include <sstream>
#include <string>

#include "comm/fmt.h"

// CBDB_TRY();
// {
//   // C++ implementation code
// }
// CBDB_CATCH_MATCH(std::exception &exp); // optional
// {
//    // specific exception handler
//    error_message.Append("error message: %s", error_message.Message());
// }
// CBDB_CATCH_DEFAULT();
// CBDB_END_TRY();
//
// CBDB_CATCH_MATCH() is optional and can have several match pattern.

// being of a try block w/o explicit handler
#define CBDB_TRY()                                          \
  do {                                                      \
    bool internal_cbdb_try_throw_error_ = false;            \
    bool internal_cbdb_try_throw_error_with_stack_ = false; \
    cbdb::ErrorMessage error_message;                       \
    try {
// begin of a catch block
#define CBDB_CATCH_MATCH(exception_decl) \
  }                                      \
  catch (exception_decl) {               \
    internal_cbdb_try_throw_error_ = true;

// catch c++ exception and rethrow ERROR to C code
// only used by the outer c++ code called by C
#define CBDB_CATCH_DEFAULT()                          \
  }                                                   \
  catch (cbdb::CException & e) {                      \
    internal_cbdb_try_throw_error_ = true;            \
    internal_cbdb_try_throw_error_with_stack_ = true; \
    global_pg_error_message = elog_message();         \
    elog(LOG, "\npax stack trace: \n%s", e.Stack());  \
    global_exception = e;                             \
  }                                                   \
  catch (...) {                                       \
    internal_cbdb_try_throw_error_ = true;            \
    internal_cbdb_try_throw_error_with_stack_ = false;

// like PG_FINALLY
#define CBDB_FINALLY(...) \
  }                       \
  {                       \
    do {                  \
      __VA_ARGS__;        \
    } while (0);

// end of a try-catch block
#define CBDB_END_TRY()                                                      \
  }                                                                         \
  if (internal_cbdb_try_throw_error_) {                                     \
    if (global_pg_error_message) {                                          \
      elog(LOG, "\npg error message:%s", global_pg_error_message);          \
    }                                                                       \
    if (internal_cbdb_try_throw_error_with_stack_) {                        \
      elog(LOG, "\npax stack trace: \n%s", global_exception.Stack());       \
      ereport(                                                              \
          ERROR,                                                            \
          errmsg("%s (PG message: %s)", global_exception.What().c_str(),    \
                 !global_pg_error_message ? "" : global_pg_error_message)); \
    }                                                                       \
    if (error_message.Length() == 0)                                        \
      error_message.Append("ERROR: %s", __func__);                          \
    ereport(ERROR, errmsg("%s", error_message.Message()));                  \
  }                                                                         \
  }                                                                         \
  while (0)

namespace cbdb {

#define DEFAULT_STACK_MAX_DEPTH 63
#define DEFAULT_STACK_MAX_SIZE \
  ((DEFAULT_STACK_MAX_DEPTH + 1) * PIPE_MAX_PAYLOAD)
#define MAX_SIZE_OF_ERROR_MESSAGE 2048
// error message buffer
class ErrorMessage final {
 public:
  ErrorMessage();
  ErrorMessage(const ErrorMessage &message);
  void Append(const char *format, ...) noexcept;
  void AppendV(const char *format, va_list ap) noexcept;
  const char *Message() const noexcept;
  int Length() const noexcept;

 private:
  int index_ = 0;
  char message_[128];
};

class CException {
 public:
  enum ExType {
    kExTypeInvalid = 0,
    kExTypeUnImplements,
    kExTypeAssert,
    kExTypeAbort,
    kExTypeOOM,
    kExTypeIOError,
    kExTypeCError,
    kExTypeLogicError,
    kExTypeInvalidMemoryOperation,
    kExTypeSchemaNotMatch,
    kExTypeInvalidPORCFormat,
    kExTypeOutOfRange,
    kExTypeFileOperationError,
    kExTypeCompressError,
    kExTypeArrowExportError,
    kExTypeArrowBatchSizeTooSmall,
    kExTypeToastNoLZ4Support,
    kExTypeToastInvalidCompressType,
    kExTypeToastPGLZError,
    kExTypeToastLZ4Error,
    kExTypeInvalidExternalToast,
    kExTypeInvalidIndexType,
    kExTypeValueOverflow,
    kExTypeEnd,
  };

  explicit CException(ExType extype);

  CException(const char *filename, int lineno, ExType extype,
             const char *message);

  const char *Filename() const;

  int Lineno() const;

  ExType EType() const;

  void AppendDetailMessage(const char *message);

  void AppendDetailMessage(const std::string &message);

  std::string What() const;

  const char *Stack() const;

  static void Raise(const char *filename, int line, ExType extype)
      __attribute__((__noreturn__));
  static void Raise(const char *filename, int line, ExType extype,
                    const char *message) __attribute__((__noreturn__));
  static void Raise(const char *filename, int line, ExType extype,
                    const std::string &message) __attribute__((__noreturn__));
  static void Raise(CException ex, bool reraise) __attribute__((__noreturn__));
  static void ReRaise(CException ex) __attribute__((__noreturn__));

 private:
  char stack_[DEFAULT_STACK_MAX_SIZE];
  const char *m_filename_;
  int m_lineno_;
  ExType m_extype_;
  char m_errormsg_[MAX_SIZE_OF_ERROR_MESSAGE] = {0};
  int m_errormsg_len_ = 0;
};

}  // namespace cbdb

extern cbdb::CException global_exception;
extern char *global_pg_error_message;

#define CBDB_RAISE(...) cbdb::CException::Raise(__FILE__, __LINE__, __VA_ARGS__)
#define CBDB_RERAISE(ex) cbdb::CException::ReRaise(ex)
#define CBDB_CHECK(check, ...) \
  do {                         \
    if (unlikely(!(check))) {  \
      CBDB_RAISE(__VA_ARGS__); \
    }                          \
  } while (0)
