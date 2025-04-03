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
 * CException.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/exceptions/CException.cc
 *
 *-------------------------------------------------------------------------
 */

#include "CException.h"

#include "comm/cbdb_wrappers.h"

#ifdef __GNUC__
#include <cxxabi.h>
#include <execinfo.h>
#endif

#include <cstdarg>
#include <cstring>
namespace cbdb {

#ifdef __GNUC__

static const char *empty_msg = "  <empty stack>\n";
static const char *func_name_msg = "  %s: %s + %s\n";
static const char *no_func_name_msg = "  %s: <symbol not found> + %s\n";
static const char *begin_name_msg = "  %s: %s() + %s\n";
static const char *symbol_name = "  %s\n";

static inline int Append(char *dst, size_t count, const char *format, ...) {
  va_list ap;
  int n;
  va_start(ap, format);
  n = vsnprintf(dst, count, format, ap);
  va_end(ap);
  // ignore n < 0 error
  // When there are other problems in logic
  // it can only be judged by the generated log
  if (n < 0) {
    n = 0;
  }
  return n;
}

static inline void StackTrace(char *stack_buffer,
                              uint8 max_depth = DEFAULT_STACK_MAX_DEPTH) {
  void *addr_list[max_depth + 1];
  int addr_len;
  char **symbol_list;
  size_t origin_func_name_size = 16384;  // same as ORCA
  size_t func_name_size = origin_func_name_size;
  char func_name[func_name_size];
  int index = 0;

  addr_len = backtrace(addr_list, sizeof(addr_list) / sizeof(void *));
  if (addr_len == 0) {
    Append(stack_buffer, DEFAULT_STACK_MAX_SIZE, empty_msg);
    return;
  }

  symbol_list = backtrace_symbols(addr_list, addr_len);
  if (!symbol_list) {
    return;
  }

  char *begin_name, *begin_offset, *end_offset;
  int single_buffer_size = 0;
  for (int i = 1; i < addr_len; i++) {
    begin_name = nullptr;
    begin_offset = nullptr;
    end_offset = nullptr;

    for (char *p = symbol_list[i]; *p; ++p) {
      if (*p == '(') {
        begin_name = p;
      } else if (*p == '+') {
        begin_offset = p;
      } else if (*p == ')' && begin_offset) {
        end_offset = p;
        break;
      }
    }

    if (begin_name && begin_offset && end_offset && begin_name < begin_offset) {
      *begin_name++ = '\0';
      *begin_offset++ = '\0';
      *end_offset = '\0';

      int status;
      char *ret =
          abi::__cxa_demangle(begin_name, func_name, &func_name_size, &status);

      if (origin_func_name_size < func_name_size) {
        // realloc happen
        // should not allow realloc in `abi::__cxa_demangle`
        // just return and make sure origin_func_name_size big enough
#pragma GCC diagnostic ignored "-Wfree-nonheap-object"
        free(func_name);  // NOLINT
        goto finish;
      }
      if (status == 0) {
        if (!ret) {
          continue;
        }
        memcpy(func_name, ret, func_name_size);
        single_buffer_size = strlen(func_name_msg) + strlen(symbol_list[i]) +
                             func_name_size + strlen(begin_offset);
        if (index > DEFAULT_STACK_MAX_SIZE ||
            DEFAULT_STACK_MAX_SIZE - index < single_buffer_size) {
          goto finish;
        }
        index += Append(stack_buffer + index, DEFAULT_STACK_MAX_SIZE - index,
                        func_name_msg, symbol_list[i], func_name, begin_offset);
      } else {
        if (begin_name[0] == '\0') {
          single_buffer_size = strlen(no_func_name_msg) +
                               strlen(symbol_list[i]) + strlen(begin_offset);
          if (index > DEFAULT_STACK_MAX_SIZE ||
              DEFAULT_STACK_MAX_SIZE - index < single_buffer_size) {
            goto finish;
          }
          index += Append(stack_buffer + index, DEFAULT_STACK_MAX_SIZE - index,
                          no_func_name_msg, symbol_list[i], begin_offset);
        } else {
          single_buffer_size = strlen(begin_name_msg) + strlen(symbol_list[i]) +
                               strlen(begin_name) + strlen(begin_offset);
          if (index >= DEFAULT_STACK_MAX_SIZE ||
              DEFAULT_STACK_MAX_SIZE - index < single_buffer_size) {
            goto finish;
          }
          index +=
              Append(stack_buffer + index, DEFAULT_STACK_MAX_SIZE - index,
                     begin_name_msg, symbol_list[i], begin_name, begin_offset);
        }
      }
    } else {
      single_buffer_size = strlen(symbol_name) + strlen(symbol_list[i]);
      if (index >= DEFAULT_STACK_MAX_SIZE ||
          DEFAULT_STACK_MAX_SIZE - index < single_buffer_size) {
        goto finish;
      }
      index += Append(stack_buffer + index, DEFAULT_STACK_MAX_SIZE - index,
                      symbol_name, symbol_list[i]);
    }
  }

finish:
  if (index < DEFAULT_STACK_MAX_SIZE) {
    stack_buffer[index] = '\0';
  }
  free(symbol_list);  // NOLINT
}

#endif

ErrorMessage::ErrorMessage() {  // NOLINT
  index_ = 0;
  message_[0] = '\0';
}

ErrorMessage::ErrorMessage(const ErrorMessage &message) {  // NOLINT
  index_ = message.index_;
  std::memcpy(message_, message.message_, static_cast<size_t>(message.index_));
}

void ErrorMessage::Append(const char *format, ...) noexcept {
  auto index = (unsigned)index_;
  if (index < sizeof(message_)) {
    va_list ap;
    int n;
    va_start(ap, format);
    n = vsnprintf(&message_[index], sizeof(message_) - index, format, ap);
    va_end(ap);
    if (n > 0) index_ += n;
  }
}

void ErrorMessage::AppendV(const char *format, va_list ap) noexcept {
  auto index = (unsigned)index_;
  if (index < sizeof(message_)) {
    int n;
    n = vsnprintf(&message_[index], sizeof(message_) - index, format, ap);
    if (n > 0) index_ += n;
  }
}

const char *ErrorMessage::Message() const noexcept { return &message_[0]; }
int ErrorMessage::Length() const noexcept { return index_; }

void CException::Raise(CException ex, bool reraise) {
#ifdef __GNUC__
  if (!reraise) {
    StackTrace(&ex.stack_[0]);
  }
#endif
  throw ex;  // NOLINT
}

CException::CException(ExType extype)  // NOLINT
    : m_filename_(nullptr), m_lineno_(0), m_extype_(extype) {}

CException::CException(const char *filename, int lineno,  // NOLINT
                       ExType extype, const char *message)
    : m_filename_(filename), m_lineno_(lineno), m_extype_(extype) {
  if (!message) {
    return;
  }

  strncpy(m_errormsg_, message, MAX_SIZE_OF_ERROR_MESSAGE - 1);
  m_errormsg_len_ = strlen(message);
  if (m_errormsg_len_ > MAX_SIZE_OF_ERROR_MESSAGE - 1) {
    m_errormsg_len_ = MAX_SIZE_OF_ERROR_MESSAGE - 1;
  }
}

const char *CException::Filename() const { return m_filename_; }

int CException::Lineno() const { return m_lineno_; }

CException::ExType CException::EType() const { return m_extype_; }

void CException::AppendDetailMessage(const char *message) {
  if (m_errormsg_len_ >= MAX_SIZE_OF_ERROR_MESSAGE - 1) {
    return;
  }

  strncpy(m_errormsg_ + m_errormsg_len_, message,
          (MAX_SIZE_OF_ERROR_MESSAGE - 1) - m_errormsg_len_);
  m_errormsg_len_ += strlen(message);
  if (m_errormsg_len_ > MAX_SIZE_OF_ERROR_MESSAGE - 1) {
    m_errormsg_len_ = MAX_SIZE_OF_ERROR_MESSAGE - 1;
  }
}

void CException::AppendDetailMessage(const std::string &message) {
  AppendDetailMessage(message.c_str());
}

const char *CException::Stack() const { return stack_; }

void CException::Raise(const char *filename, int lineno, ExType extype) {
  Raise(CException(filename, lineno, extype, ""), false);
}

void CException::Raise(const char *filename, int lineno, ExType extype,
                       const char *message) {
  Raise(CException(filename, lineno, extype, message), false);
}

void CException::Raise(const char *filename, int lineno, ExType extype,
                       const std::string &message) {
  Raise(CException(filename, lineno, extype, message.c_str()), false);
}

void CException::ReRaise(CException ex) { Raise(ex, true); }

static const char *exception_names[] = {
    "Invalid ExType",
    "Not implements in cpp",
    "Assert Failure",
    "Abort",
    "Out of Memory",
    "IO Error",
    "C ERROR",
    "Logic ERROR",
    "Invalid memory operation",
    "Schema not match",
    "Invalid porc format",
    "Out of range",
    "File operation got error",
    "Compress got errors",
    "Arrow export got errors",
    "Arrow record batch too small",
    "no LZ4 support",
    "Invalid toast compress type",
    "Toast pglz got error",
    "Toast lz4 got error",
    "Invalid external toast",
    "Invalid index type",
    "Value overflow",
};

// don't forge regsiter the description of the exception
static_assert(lengthof(exception_names) == CException::ExType::kExTypeEnd,
              "exception description not match");

std::string CException::What() const {
  std::ostringstream buffer;
  buffer << m_filename_ << ":" << m_lineno_ << " " << exception_names[m_extype_]
         << " detail: " << m_errormsg_;
  return buffer.str();
}

}  // namespace cbdb

cbdb::CException global_exception(cbdb::CException::kExTypeInvalid);
char *global_pg_error_message = nullptr;
