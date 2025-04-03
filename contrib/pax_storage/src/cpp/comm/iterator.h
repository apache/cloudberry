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
 * iterator.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/iterator.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "comm/cbdb_api.h"

namespace pax {
template <typename T>
class IteratorBase {
 public:
  virtual bool HasNext() = 0;
  virtual T Next() = 0;
  virtual void Rewind() = 0;
  virtual void Release() {}; // empty release
  virtual ~IteratorBase() = default;
};  // class IteratorBase

// FilterIterator: wrap an iterator that may have a filter whether to pass
// the value from internal iterator. If the qual function returns true,
// the current item will return to the caller, otherwise the current item
// is ignored.
template <typename T>
class FilterIterator : public IteratorBase<T> {
 public:
 FilterIterator(std::unique_ptr<IteratorBase<T>> &&it, std::function<bool(const T &x)> &&qual)
  : it_(std::move(it)), qual_(std::move(qual)) {
    Assert(it_);
    Assert(qual_);
  }

  bool HasNext() override {
    if (valid_value_) return true;
    while (it_->HasNext()) {
      value_ = std::move(it_->Next());
      if (qual_(value_)) {
        valid_value_ = true;
        break;
      }
    }
    return valid_value_;
  }

  T Next() override {
    Assert(valid_value_);
    valid_value_ = false;
    return std::move(value_);
  }

  void Rewind() override {
    it_->Rewind();
    valid_value_ = false;
  }

  void Release() override {
    it_->Release();
    valid_value_ = false;
  }

  virtual ~FilterIterator() = default;

 protected:
  std::unique_ptr<IteratorBase<T>> it_;
  std::function<bool(const T &x)> qual_;
  T value_;
  bool valid_value_ = false;
};

template <typename T>
class VectorIterator : public IteratorBase<T> {
 public:
  explicit VectorIterator(std::vector<T> &&v): v_(std::move(v)){}
  virtual ~VectorIterator() = default;

  bool HasNext() override { return index_ < v_.size(); }
  T Next() override {
    Assert(HasNext());
    return v_[index_++];
  }
  void Rewind() override { index_ = 0; }

  void Release() override { index_ = v_.size(); }

 protected:
  std::vector<T> v_;
  size_t index_ = 0;
};

}  // namespace pax
