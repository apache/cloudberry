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
 * singleton.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/singleton.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

namespace pax {

template <typename T>
class Singleton final {
 public:
  Singleton(const Singleton &) = delete;
  Singleton &operator=(const Singleton &) = delete;

  template <typename... Args>
  inline static T *GetInstance(Args &&...args) {
    static T instance{std::forward<Args>(args)...};
    return &instance;
  }

 protected:
  Singleton() = default;
  ~Singleton() = default;
};

}  // namespace pax
